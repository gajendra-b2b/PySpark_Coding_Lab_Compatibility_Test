from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum, desc, round

class SalesAnalysis:
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    # Task 1: Load CSV file from local storage as DataFrame
    def load_data(self, path: str) -> DataFrame:
        pass
        # Write your code below

