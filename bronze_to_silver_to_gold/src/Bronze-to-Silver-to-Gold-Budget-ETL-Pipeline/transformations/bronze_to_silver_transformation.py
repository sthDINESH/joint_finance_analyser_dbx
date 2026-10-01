from pyspark import pipelines as dp
from pyspark.sql.functions import to_date, expr, col, lit, when, lower

from bronze_to_silver_to_gold.src.config.categories import categories

@dp.materialized_view(
    name="joint_account_budget_silver.joint_budget",
    comment="Cleaned joint statements transformed from bronze to silver",
)
def joint_budget_silver():
    df = spark.read.table("workspace.joint_account_budget_bronze.joint_budget")

    # Drop unnecessary columns
    df = df.drop("_gdrive_metadata", "_file_metadata")

    # Rename and cast date_month
    df = df.withColumnRenamed("date_month", "date")
    df = df.withColumn("date", to_date(df["date"], "MM-yyyy"))

    # Unpivot columns into rows
    df = df.select(
        "date",
        expr("""
            stack(12,
                'mortgage', cast(mortage as double),
                'broadband_tv', cast(broadband_tv as double),
                'council_tax', cast(council_tax as double),
                'tv_licence', cast(tv_licence as double),
                'electricity_gas', cast(electricity_gas as double),
                'water', cast(water as double),
                'home_insurance', cast(home_insurance as double),
                'aviva_insurance_i', cast(aviva_insurance_i as double),
                'aviva_insurance_ii', cast(aviva_insurance_ii as double),
                'groceries', cast(groceries as double),
                'roof_loan', cast(roof_loan as double),
                'boiler_loan', cast(boiler_loan as double)
            ) as (name, amount)
        """)
    )

    

    # Build a chained when() expression — Spark evaluates conditions in order,
    # so the first matching category wins
    desc_lower = lower(col("name"))
    category_expr = None

    for category, map in categories.items():
        # OR together all keywords for this category
        condition = None
        # Skip category if keywords for budget is empty
        if map.get("budget") is None:
            continue
        for keyword in map["budget"]:
            check = desc_lower.contains(keyword.lower())
            condition = check if condition is None else condition | check
        if category_expr is None:
            category_expr = when(condition, lit(category))
        else:
            category_expr = category_expr.when(condition, lit(category))

    category_expr = category_expr.otherwise(lit("other"))

    df = df.withColumn("category", category_expr)
    
    return df


