from pyspark.sql.functions import to_date, col, lower, when, lit
from pyspark import pipelines as dp

from bronze_to_silver_to_gold.src.config.categories import categories

@dp.materialized_view(
    name="joint_statements_silver.joint_statements",
    comment="Cleaned joint statements transformed from bronze to silver",
)
def joint_statements_silver():
    df = spark.read.table("workspace.joint_statements_bronze.joint_statements_google_drive")

    # Drop unnecessary columns
    df = df.drop("Sort Code", "Account Number", "_gdrive_metadata", "_file_metadata")

    # Rename columns to remove spaces
    df = df.withColumnsRenamed({
        "Transaction Date": "date",
        "Transaction Type": "type",
        "Transaction Description": "description",
        "Debit Amount": "debit",
        "Credit Amount": "credit",
        "Balance": "balance",
    })

    # Cast to appropriate data types
    df = (
        df.withColumn("date", to_date(df["date"], "dd/MM/yyyy"))
        .withColumn("debit", df["debit"].cast("double"))
        .withColumn("credit", df["credit"].cast("double"))
        .withColumn("balance", df["balance"].cast("double"))
    )

    # Add a category column

    # Build a chained when() expression — Spark evaluates conditions in order,
    # so the first matching category wins
    desc_lower = lower(col("description"))
    category_expr = None

    for category, map in categories.items():
        # OR together all keywords for this category
        condition = None
        for keyword in map["statement"]:
            check = desc_lower.contains(keyword.lower())
            condition = check if condition is None else condition | check
        if category == "incomings":
            condition = condition & col("credit").isNotNull()
        if category == "widthdrawals":
            condition = condition & col("debit").isNotNull()
        if category_expr is None:
            category_expr = when(condition, lit(category))
        else:
            category_expr = category_expr.when(condition, lit(category))

    category_expr = category_expr.otherwise(lit("other"))

    df = df.withColumn("category", category_expr)

    return df
