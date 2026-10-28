# =========================================================================
# NOTEBOOK: 02_bronze_to_silver_scd2_pyspark.py
# PURPOSE: Cleanse Bronze HRP Data & Apply SCD Type 2 Merge into Silver Delta
# =========================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("HRP_Bronze_To_Silver_SCD2").getOrCreate()

# Paths in ADLS Gen2
bronze_path = "abfss://bronze@sthrplakehouse.dfs.core.windows.net/hrp/payroll_transactions/"
silver_emp_path = "abfss://silver@sthrplakehouse.dfs.core.windows.net/hrp/dim_employee_scd2/"
silver_payroll_path = "abfss://silver@sthrplakehouse.dfs.core.windows.net/hrp/fact_payroll_clean/"

# 1. Read Incremental Batch from Bronze Parquet
df_bronze = spark.read.format("parquet").load(bronze_path)

# 2. Data Cleansing, Standardization & Net Pay Calculation
df_cleaned = (
    df_bronze
    .filter(F.col("emp_id").isNotNull())
    .withColumn("emp_id", F.upper(F.trim(F.col("emp_id"))))
    .withColumn("department", F.initcap(F.trim(F.col("department"))))
    .withColumn("gross_pay", F.round(F.col("base_salary") + F.col("overtime_pay"), 2))
    .withColumn("net_pay", F.round(F.col("base_salary") + F.col("overtime_pay") - F.col("tax_deducted"), 2))
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

# 3. Deduplicate Bronze Batch using Window Function (Keep Latest Record per Emp + PayPeriod)
dedup_window = Window.partitionBy("emp_id", "pay_period").orderBy(F.col("last_modified_date").desc())
df_deduped = (
    df_cleaned
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Write Cleansed Transactions to Silver Payroll Delta Table (Upsert)
df_deduped.write.format("delta").mode("append").save(silver_payroll_path)

# 4. Extract Latest Employee Profile for SCD Type 2 Dimension Merge
emp_latest_win = Window.partitionBy("emp_id").orderBy(F.col("last_modified_date").desc())
df_emp_updates = (
    df_deduped
    .withColumn("rn", F.row_number().over(emp_latest_win))
    .filter(F.col("rn") == 1)
    .select(
        "emp_id", "emp_name", "department", "country", "base_salary",
        F.col("last_modified_date").alias("effective_start_date")
    )
    .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

# Initialize or Perform SCD Type 2 Merge on Silver Employee Dimension
if DeltaTable.isDeltaTable(spark, silver_emp_path):
    delta_emp = DeltaTable.forPath(spark, silver_emp_path)
    
    # Close out existing current records where Department or Base Salary changed
    delta_emp.alias("target").merge(
        df_emp_updates.alias("source"),
        "target.emp_id = source.emp_id AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.department <> source.department OR target.base_salary <> source.base_salary",
        set={
            "effective_end_date": "source.effective_start_date",
            "is_current": "false"
        }
    ).execute()
else:
    df_emp_updates.write.format("delta").mode("overwrite").save(silver_emp_path)
