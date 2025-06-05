from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum, desc, round

class SalesAnalysis:
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    # Task 1: Create SparkSession for app "Sales Data Analysis"

    def init_spark_session(self) -> SparkSession:
        pass

    # Task 2: Load CSV file from local storage as DataFrame
    def load_data(self, path: str) -> DataFrame:
        pass

    # Task 3: Verify schema of the DataFrame
    def display_schema(self, df: DataFrame) -> None:
        pass

    # Task 4: Display entries for country "Germany"
    def filter_by_country(self, df: DataFrame, country: str = "Germany") -> DataFrame:
        pass

    # Task 5: Find total number of unique products
    def total_products(self, df: DataFrame) -> int:
        pass

    # Task 6: Find top N products by sales
    def top_n_products(self, df: DataFrame, n: int) -> DataFrame:
        pass

    # Task 7: Calculate total sales across all products
    def total_sales(self, df: DataFrame) -> int:
        pass
        
    # Task 8: Find individual market share of all products
    def market_share(self, df: DataFrame) -> DataFrame:
        pass
