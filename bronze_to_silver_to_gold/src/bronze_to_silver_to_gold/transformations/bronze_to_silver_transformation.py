from pyspark.sql.functions import to_date
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

    return df
