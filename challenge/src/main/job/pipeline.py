from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum, desc, round

class SalesAnalysis:

    # Task 1: Create SparkSession for app "Sales Data Analysis"
    def init_spark_session(self) -> SparkSession:
        return SparkSession.builder.appName("Sales Data Analysis").getOrCreate()

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # Task 2: Load CSV file from local storage as DataFrame
    def load_data(self, path: str) -> DataFrame:
        return self.spark.read.csv(path, header=True, inferSchema=True)

    # Task 3: Verify schema of the DataFrame
    def display_schema(self, df: DataFrame) -> None:
        df.printSchema()

    # Task 4: Display entries for country "Germany"
    def filter_by_country(self, df: DataFrame, country: str = "Germany") -> DataFrame:
        return df.filter(col("country") == country)

    # Task 5: Find total number of unique products
    def total_products(self, df: DataFrame) -> int:
        return df.select("product_id").distinct().count()

    # Task 6: Find top N products by sales
    def top_n_products(self, df: DataFrame, n: int) -> DataFrame:
        return (df.groupBy("product")
                  .agg(sum("sales").alias("total_sales"))
                  .orderBy(desc("total_sales"))
                  .limit(n))

    # Task 7: Calculate total sales across all products
    def total_sales(self, df: DataFrame) -> int:
        return df.agg(sum("sales")).first()[0]

    # Task 8: Find individual market share of all products
    def market_share(self, df: DataFrame) -> DataFrame:
        total = df.agg(sum("sales").alias("total")).collect()[0]["total"]
        return (df.groupBy("product")
                  .agg(sum("sales").alias("product_sales"))
                  .withColumn("market_share", round(col("product_sales") / total * 100, 2))
                  .orderBy(desc("market_share")))
