from pyspark import pipelines as dp

@dp.materialized_view(
    name="joint_statements_gold.joint_statements",
    comment="Cleaned joint statements transformed from silver to gold",
)
def joint_statements_silver():
    return spark.read.table("workspace.joint_statements_silver.joint_statements")