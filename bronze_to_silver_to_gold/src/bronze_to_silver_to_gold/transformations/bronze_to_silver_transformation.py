from pyspark.sql.functions import to_date, col, lower, when, lit
from pyspark import pipelines as dp


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

    categories = {
        "mortgage": ["Leeds Building Soc"],
        "groceries": [
            "sainsburys",
            "lidl",
            "marks&spencer",
            "aldi",
            "tesco",
            "morrisons",
        ],
        "utilities": [
            "octopus",
            "yorkshire water",
        ],
        "insurances": [
            "aviva",
            "home insurance",
        ],
        "loans": [
            "novuna personal",
            "lloyds bank loan",
        ],
        "council tax": [
            "sheffield city",
        ],
        "tv licence": [
            "tv licence",
        ],
        "incomings": [
            "d sthapit",
            "c rimmer",
        ],
    }

    # Build a chained when() expression — Spark evaluates conditions in order,
    # so the first matching category wins
    desc_lower = lower(col("description"))
    category_expr = None

    for category, keywords in categories.items():
        # OR together all keywords for this category
        condition = None
        for keyword in keywords:
            check = desc_lower.contains(keyword.lower())
            condition = check if condition is None else condition | check
        if category_expr is None:
            category_expr = when(condition, lit(category))
        else:
            category_expr = category_expr.when(condition, lit(category))

    category_expr = category_expr.otherwise(lit("other"))

    df = df.withColumn("category", category_expr)

    return df
