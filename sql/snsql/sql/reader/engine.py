class Engine:
    BIGQUERY = "BigQuery"
    PANDAS = "Pandas"
    POSTGRES = "Postgres"
    PRESTO = "Presto"
    REDSHIFT = "Redshift"
    SPARK = "Spark"
    SQL_SERVER = "SqlServer"

    known_engines = {BIGQUERY, PANDAS, POSTGRES, PRESTO, REDSHIFT, SPARK, SQL_SERVER}
    class_map = {BIGQUERY: "bigquery", PANDAS: "pandas", POSTGRES: "postgres", PRESTO: "presto", REDSHIFT: "redshift", SPARK: "spark", SQL_SERVER: "sql_server"}