from pyspark import pipelines as dp
from pyspark.sql.functions import year, month, sum, col, lag, when
from pyspark.sql.window import Window

@dp.materialized_view(
    name="joint_statements_gold.joint_statements",
    comment="Cleaned joint statements transformed from silver to gold",
)
def joint_statements_silver():
    silver_df = spark.read.table("workspace.joint_statements_silver.joint_statements")

    # Base aggregation: Calculate monthly expenses
    monthly_expenses_df = silver_df.groupBy(
            year("date").alias("year"),
            month("date").alias("month"),
            "category"
        ).agg(
            sum("debit").alias("outgoings"),
            sum("credit").alias("incomings"),
        )     

    # window specification for lag function
    category_window = Window.partitionBy("category").orderBy("year", "month")

    # calculate lag and change from previous
    df = monthly_expenses_df.withColumn(
            "previous_outgoings",
            lag("outgoings", 1).over(category_window),
        ).withColumn(
            "previous_incomings",
            lag("incomings").over(category_window),
        ).withColumn(
            "change_in_outgoings",
            when(
                col("previous_outgoings").isNull(), 0
            ).otherwise(
                col("outgoings") - col("previous_outgoings")
            )
        ).withColumn(
            "change_in_incomings",
            when(
                col("previous_incomings").isNull(), 0
            ).otherwise(
                col("incomings") - col("previous_incomings")
            )
        )

    return df

