# =========================================================================
# NOTEBOOK: 03_silver_to_gold_payroll_kpis.py
# PURPOSE: Build Gold Star-Schema Aggregations for Power BI & SSRS Reporting
# =========================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("HRP_Silver_To_Gold_KPIs").getOrCreate()

silver_payroll_path = "abfss://silver@sthrplakehouse.dfs.core.windows.net/hrp/fact_payroll_clean/"
gold_dept_kpi_path = "abfss://gold@sthrplakehouse.dfs.core.windows.net/hrp/agg_department_payroll_kpis/"

df_silver = spark.read.format("delta").load(silver_payroll_path)

# Calculate Department & Country Level Monthly Payroll KPIs + MoM Cost Variance
dept_mom_window = Window.partitionBy("country", "department").orderBy("pay_period")

df_gold_kpis = (
    df_silver
    .groupBy("pay_period", "country", "department")
    .agg(
        F.countDistinct("emp_id").alias("active_headcount"),
        F.round(F.sum("base_salary"), 2).alias("total_base_salary"),
        F.round(F.sum("overtime_pay"), 2).alias("total_overtime_pay"),
        F.round(F.sum("gross_pay"), 2).alias("total_gross_payroll"),
        F.round(F.sum("net_pay"), 2).alias("total_net_payroll"),
        F.sum("leave_days").alias("total_leave_days")
    )
    .withColumn("prev_month_payroll", F.lag("total_gross_payroll", 1).over(dept_mom_window))
    .withColumn(
        "mom_payroll_growth_pct",
        F.round(
            ((F.col("total_gross_payroll") - F.col("prev_month_payroll")) / F.col("prev_month_payroll")) * 100, 2
        )
    )
)

# Write to Gold Delta Table for Power BI DirectLake / SSRS consumption
df_gold_kpis.write.format("delta").mode("overwrite").save(gold_dept_kpi_path)
