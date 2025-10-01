import pytest
from pyspark.sql import SparkSession
from main.job.pipeline import SalesAnalysis
import os

spark = SparkSession.builder.master("local").appName("Test Sales").getOrCreate()

job = SalesAnalysis(spark)

# Load test data
DATA_PATH = "data/products.csv"
def load_data(path):
    return spark.read.csv(path, header=True, inferSchema=True)

df = load_data(DATA_PATH)



@pytest.mark.filterwarnings("ignore")
def test_load_data():
    # Task 2: Loaded DataFrame should not be empty
    df = job.load_data(DATA_PATH)
    assert df.count() == 50
    assert "product" in df.columns
