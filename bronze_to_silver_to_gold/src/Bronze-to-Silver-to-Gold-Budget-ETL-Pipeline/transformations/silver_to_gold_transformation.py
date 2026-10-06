from pyspark import pipelines as dp
from pyspark.sql.functions import sum, year, month, col, lag, when
from pyspark.sql.window import Window

@dp.materialized_view(
    name="joint_account_budget_gold.joint_budget",
    comment="Cleaned joint statements transformed from silver to gold",
)
def joint_budget_gold():
    silver_df = spark.read.table("workspace.joint_account_budget_silver.joint_budget")

    df = silver_df.groupBy(
        year(silver_df["date"]).alias("year"), 
        month(silver_df["date"]).alias("month"),
        "category",
    ).agg(
        sum("amount").alias("budget")
    )

    # Calculate change in budget
    # window specification for lag function
    category_window = Window.partitionBy("category").orderBy("year", "month")

    # calculate lag and change from previous
    df = df.withColumn(
            "previous_budget",
            lag("budget", 1).over(category_window),
        ).withColumn(
            "change_in_budget",
            when(
                col("previous_budget").isNull(), 0
            ).otherwise(
                col("budget") - col("previous_budget")
            )
        )

    return df