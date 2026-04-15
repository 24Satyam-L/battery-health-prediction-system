from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("BatteryPipeline").getOrCreate()


meta_df = spark.read.csv("C:\\Projects\\Battery Engineering\\extra_infos\\Inventory.csv",header=True,inferSchema=True)


meta_df = meta_df.withColumnRenamed("File Name", "file_name")  \
    .withColumnRenamed("Test Type", "test_type")  \
    .withColumnRenamed("Cycle ID", "cycle_id") \
    .withColumnRenamed("Battery ID", "battery_id") \
    .withColumnRenamed("File Path", "file_path") \
    .withColumnRenamed("capacity", "Capacity") 


meta_df = meta_df.withColumn("capacity",col("capacity").cast("double"))
    

charge_meta = meta_df.filter(col("test_type") == "charge")


charge_meta = charge_meta.withColumn("full_path",concat_ws("\\", col("file_path"), col("file_name")))


paths = [row["full_path"] for row in charge_meta.select("full_path").collect()]

df = spark.read.csv(paths, header=True, inferSchema=True)

df = df.withColumn("source_file", input_file_name())

from pyspark.sql.functions import element_at, split

# Splits by '/' and takes the last element (-1)
df = df.withColumn("file_name", element_at(split(col("source_file"), "/"), -1))

final_df =df.join(charge_meta,on="file_name",how="left")


window_spec = Window.partitionBy("battery_id", "cycle_id").orderBy("Time")
final_df = final_df.withColumn("delta_time",col("Time") - lag("Time").over(window_spec))

final_df = final_df.withColumn("delta_time", when(col("delta_time").isNull(), 0).otherwise(col("delta_time")))



final_df = final_df.withColumn("delta_voltage", col("Voltage_measured") - lag("Voltage_measured").over(window_spec))
final_df = final_df.withColumn("dV/dt", when(col("delta_time") != 0, col("delta_voltage") / col("delta_time")).otherwise(0))
final_df = final_df.withColumn("start_temperature", first("Temperature_measured").over(window_spec))
final_df = final_df.withColumn("end_temperature", last("Temperature_measured").over(window_spec))
final_df = final_df.withColumn("charge_ah",col("delta_time") * col("Current_measured")/ 3600)

final_df = final_df.withColumn("energy_wh", col("charge_ah") * col("Voltage_measured"))

filtered_df = final_df.filter(col("Current_measured") >= 0.02)


charge_summary = filtered_df.groupBy("battery_id", "cycle_id").agg(
    
    sum("charge_ah").alias("total_ah"),
    sum("energy_wh").alias("total_wh"),
    
    (max("Time") - min("Time")).alias("cycle_duration"),
    
    max("Voltage_measured").alias("max_voltage"),
    min("Voltage_measured").alias("min_voltage"),
    avg("Voltage_measured").alias("avg_voltage"),
    
    max("Temperature_measured").alias("max_temp"),
    min("Temperature_measured").alias("min_temp"),
    avg("Temperature_measured").alias("avg_temp"),
    
    max("Current_measured").alias("max_current"),
    min("Current_measured").alias("min_current"),
    avg("Current_measured").alias("avg_current"),
    max("dV/dt").alias("max_dVdt"),
    min("dV/dt").alias("min_dVdt"),
    max("ambient_temperature").alias("ambient_temp"),
    max("start_temperature").alias("start_temp"),
    max("end_temperature").alias("end_temp")

)
charge_summary = charge_summary.withColumn("Rise_temp_per_Sec",when(col("cycle_duration") != 0, (col("end_temp") - col("start_temp")) / col("cycle_duration")).otherwise(0))

charge_summary.show(5)
charge_summary.printSchema()

# =========================
# DISCHARGE PIPELINE
# =========================

#same as above but filter for discharge and calculate discharge specific features like discharge_ah, discharge_wh, etc. Then aggregate similarly to get discharge_summary.

# =========================
# DISCHARGE PIPELINE
# =========================


# 1. Filter discharge metadata
discharge_meta = meta_df.filter(col("test_type") == "discharge")
cutoff_df = spark.read.csv("C:\\Projects\\Battery Engineering\\extra_infos\\Battery Cutoff Voltage.csv",
header=True,inferSchema=True)

cutoff_df = cutoff_df.withColumnRenamed("Battery ID", "battery_id") \
                     .withColumnRenamed("Cutoff Voltage", "cutoff_voltage")

# 2. Create full file paths
discharge_meta = discharge_meta.withColumn(
    "full_path",
    concat_ws("\\", col("file_path"), col("file_name"))
)

# 3. Collect paths (same as charge)
paths_dch = [row["full_path"] for row in discharge_meta.select("full_path").collect()]

# 4. Load discharge files
df_dch = spark.read.csv(paths_dch, header=True, inferSchema=True)


# 5. Add source file tracking
df_dch = df_dch.withColumn("source_file", input_file_name())

df_dch = df_dch.withColumn("file_name",element_at(split(col("source_file"), "/"), -1))

# 6. Join metadata
df_dch = df_dch.join(discharge_meta, on="file_name", how="left")
df_dch = df_dch.join(cutoff_df, on="battery_id", how="left")

# 7. Window spec (reuse if already defined)
window_spec = Window.partitionBy("battery_id", "cycle_id").orderBy("Time")

# 8. Feature Engineering

# Delta Time
df_dch = df_dch.withColumn(
    "delta_time",
    col("Time") - lag("Time", -1).over(window_spec)
)

df_dch = df_dch.withColumn(
    "delta_time",
    when(col("delta_time").isNull(), 0).otherwise(col("delta_time"))
)

# Delta Voltage
df_dch = df_dch.withColumn(
    "delta_voltage",
    col("Voltage_measured") - lag("Voltage_measured", -1).over(window_spec)
)

# dV/dt
df_dch = df_dch.withColumn(
    "dVdt",
    when(col("delta_time") != 0, col("delta_voltage") / col("delta_time")).otherwise(0)
)

# Temperature start & end (ordered)
df_dch = df_dch.withColumn(
    "start_temp",
    first("Temperature_measured").over(window_spec)
)

df_dch = df_dch.withColumn(
    "end_temp",
    last("Temperature_measured").over(window_spec)
)

# 9. Apply Discharge Filters

filtered_dch = df_dch.filter(
    (col("Current_measured") < -0.05) &
    (col("Voltage_measured") > col("cutoff_voltage"))
)

# 10. Compute Discharge Ah and Energy
filtered_dch = filtered_dch.withColumn(
    "discharge_ah",
    col("Current_measured") * col("delta_time") / 3600
)

filtered_dch = filtered_dch.withColumn(
    "energy_wh",
    col("discharge_ah") * col("Voltage_measured")
)

# 11. Aggregation

discharge_summary = filtered_dch.groupBy("battery_id", "cycle_id").agg(
    
    (sum("discharge_ah") * -1).alias("total_ah"),
    (sum("energy_wh") * -1).alias("total_wh"),
    
    (max("Time") - min("Time")).alias("cycle_duration"),
    
    max("Voltage_measured").alias("max_voltage"),
    min("Voltage_measured").alias("min_voltage"),
    avg("Voltage_measured").alias("avg_voltage"),
    
    max("Temperature_measured").alias("max_temp"),
    min("Temperature_measured").alias("min_temp"),
    avg("Temperature_measured").alias("avg_temp"),
    
    max("Current_measured").alias("max_current"),
    min("Current_measured").alias("min_current"),
    avg("Current_measured").alias("avg_current"),
    
    max("dVdt").alias("max_dVdt"),
    min("dVdt").alias("min_dVdt"),
    
    max("ambient_temperature").alias("ambient_temp"),
    
    max("start_temp").alias("start_temp"),
    max("end_temp").alias("end_temp")
)

# 12. Temperature Rise Rate
discharge_summary = discharge_summary.withColumn(
    "Rise_temp_per_Sec",when(col("cycle_duration") != 0,
    (col("end_temp") - col("start_temp")) / col("cycle_duration")).otherwise(0)
)


#-----------------Merging Charge and Discharge Summaries-----------------

win = Window.partitionBy("battery_id").orderBy("cycle_id")
charge_summary = charge_summary.withColumn("cycle_id", row_number().over(win))
discharge_summary = discharge_summary.withColumn("cycle_id", row_number().over(win))

min_chg = charge_summary.groupBy("battery_id").agg(min("cycle_id").alias("min_chg"))

min_dch = discharge_summary.groupBy("battery_id").agg(min("cycle_id").alias("min_dch"))

min_df = min_chg.join(min_dch, on="battery_id")

min_df = min_df.withColumn("shift_flag",when(col("min_chg") > col("min_dch"), 1).otherwise(0))

discharge_summary = discharge_summary.join(min_df.select("battery_id", "shift_flag"), on="battery_id")

discharge_summary = discharge_summary.withColumn("cycle_id",col("cycle_id") - col("shift_flag"))



charge_summary = charge_summary.selectExpr(
    "battery_id",
    "cycle_id",
    "total_ah as charge_ah",
    "total_wh as charge_wh",
    "cycle_duration as charge_duration",
    "avg_voltage as charge_avg_voltage",
    "max_voltage as charge_max_voltage",
    "min_voltage as charge_min_voltage",
    "max_temp as charge_max_temp",
    "Rise_temp_per_Sec as charge_temp_rise"
)

discharge_summary = discharge_summary.selectExpr(
    "battery_id",
    "cycle_id",
    "total_ah as discharge_ah",
    "total_wh as discharge_wh",
    "cycle_duration as discharge_duration",
    "avg_voltage as discharge_avg_voltage",
    "max_voltage as discharge_max_voltage",
    "min_voltage as discharge_min_voltage",
    "max_temp as discharge_max_temp",
    "Rise_temp_per_Sec as discharge_temp_rise"
)

final_summary = charge_summary.join(discharge_summary,on=["battery_id", "cycle_id"],how="inner")

rename_dict = {
    "charge_ah": "charge_capacity_ah",
    "charge_wh": "charge_energy_wh",
    "charge_duration": "charge_duration_sec",
    "charge_avg_voltage": "charge_voltage_avg",
    "charge_max_voltage": "charge_voltage_max",
    "charge_min_voltage": "charge_voltage_min",
    "charge_max_temp": "charge_temp_max",
    "charge_temp_rise": "charge_temp_rise",

    "discharge_ah": "discharge_capacity_ah",
    "discharge_wh": "discharge_energy_wh",
    "discharge_duration": "discharge_duration_sec",
    "discharge_avg_voltage": "discharge_voltage_avg",
    "discharge_max_voltage": "discharge_voltage_max",
    "discharge_min_voltage": "discharge_voltage_min",
    "discharge_max_temp": "discharge_temp_max",
    "discharge_temp_rise": "discharge_temp_rise"
}

for old, new in rename_dict.items():
    final_summary = final_summary.withColumnRenamed(old, new)


# --- Coulombic Efficiency ---
final_summary = final_summary.withColumn(
    "coulombic_efficiency",
    try_divide(col("discharge_capacity_ah"), col("charge_capacity_ah"))
)

# --- Energy Efficiency ---
final_summary = final_summary.withColumn(
    "energy_efficiency",
    try_divide(col("discharge_energy_wh"), col("charge_energy_wh"))
)

# --- SOH (State of Health) ---
window_spec = Window.partitionBy("battery_id").orderBy("cycle_id")

final_summary = final_summary.withColumn(
    "initial_capacity",
    first("discharge_capacity_ah").over(window_spec)
)

final_summary = final_summary.withColumn(
    "soh",
    try_divide(col("discharge_capacity_ah"), col("initial_capacity"))
)
final_summary = final_summary.withColumn(
    "prev_soh",
    lag("soh").over(window_spec)
)

final_summary = final_summary.withColumn(
    "capacity_fade",
    col("prev_soh") - col("soh")
)

final_summary = final_summary.withColumn(
    "coulombic_efficiency",
    col("coulombic_efficiency") * 100
)

final_summary = final_summary.withColumn(
    "energy_efficiency",
    col("energy_efficiency") * 100
)

final_summary = final_summary.withColumn(
    "soh",
    col("soh") * 100
)

final_summary = final_summary.select(
    "battery_id",
    "cycle_id",
    "charge_capacity_ah",
    "discharge_capacity_ah",
    "charge_energy_wh",
    "discharge_energy_wh",
    "coulombic_efficiency",
    "energy_efficiency",
    "soh",
    "capacity_fade",
    "charge_duration_sec",
    "discharge_duration_sec",
    "charge_voltage_avg",
    "discharge_voltage_avg",
    "charge_temp_max",
    "discharge_temp_max"
)

final_summary = final_summary.orderBy("battery_id", "cycle_id")


final_summary.orderBy("battery_id", "cycle_id") \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("C:/Projects/Battery Engineering/Summary Files/Spark_battery_dataset_csv")


