import pytest
from pyspark.sql import SparkSession
from main.job.pipeline import SalesAnalysis
import os

# Initialize Spark and job
spark = SparkSession.builder.master("local").appName("Test Sales").getOrCreate()
job = SalesAnalysis(spark)

# Load test data
DATA_PATH = "../../data/products.csv"
df = job.load_data(DATA_PATH)


@pytest.mark.filterwarnings("ignore")
def test_spark_session_creation():
    # Task 1: SparkSession should not be None
    session = job.init_spark_session()
    assert session is not None

@pytest.mark.filterwarnings("ignore")
def test_load_data():
    # Task 2: Loaded DataFrame should not be empty
    assert df.count() > 0
@pytest.mark.filterwarnings("ignore")
def test_display_schema():
    # Task 3: Schema should include expected fields
    schema_fields = set(field.name for field in df.schema.fields)
    assert {"product_id", "product", "country", "sales"}.issubset(schema_fields)

@pytest.mark.filterwarnings("ignore")
def test_filter_by_country():
    # Task 4: Should only return rows with country == Germany
    germany_df = job.filter_by_country(df, "Germany")
    assert all(row['country'] == "Germany" for row in germany_df.collect())

@pytest.mark.filterwarnings("ignore")
def test_total_products():
    # Task 5: Total distinct products should be <= 5
    count = job.total_products(df)
    assert count == 5

@pytest.mark.filterwarnings("ignore")
def test_top_n_products():
    # Task 6: Should return exactly 3 products
    top3 = job.top_n_products(df, 3)
    assert top3.count() == 3

@pytest.mark.filterwarnings("ignore")
def test_total_sales():
    # Task 7: Should return total sales > 0
    total = job.total_sales(df)
    assert total > 0

@pytest.mark.filterwarnings("ignore")
def test_market_share():
    # Task 8: Should return market share sum ≈ 100%
    market_df = job.market_share(df)
    total_share = sum(row["market_share"] for row in market_df.collect())
    assert abs(total_share - 100.0) < 1e-2



