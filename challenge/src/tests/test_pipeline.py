import pytest
from pyspark.sql import SparkSession
from main.job.pipeline import SalesAnalysis
import os

spark = SparkSession.builder.master("local").appName("Test Sales").getOrCreate()

job = SalesAnalysis(spark)

# Load test data
DATA_PATH = "../../data/products.csv"
def load_data(path):
    return spark.read.csv(path, header=True, inferSchema=True)

df = load_data(DATA_PATH)


@pytest.mark.filterwarnings("ignore")
def test_init_spark_session():
    # Task 1: SparkSession should not be None
    job1 = SalesAnalysis(None)
    session = job1.init_spark_session()
    assert isinstance(session, SparkSession)

@pytest.mark.filterwarnings("ignore")
def test_load_data():
    # Task 2: Loaded DataFrame should not be empty
    df = job.load_data(DATA_PATH)
    assert df.count() == 50
    assert "product" in df.columns

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
    top3 = job.top_n_products(df, 3).collect()
    assert len(top3) == 3
    assert top3[1]['product'] == "Product C"

@pytest.mark.filterwarnings("ignore")
def test_total_sales():
    # Task 7: Should return total sales > 0
    total = job.total_sales(df)
    assert total == 5767

@pytest.mark.filterwarnings("ignore")
def test_market_share():
    market_df = job.market_share(df)
    market_data = {row["product"]: round(row["market_share"], 2) for row in market_df.collect()}
    
    # Manually computed expected values based on 5627 total sales
    expected_market_shares = {
        "Product A": 28.81,
        "Product B": 17.6,
        "Product C": 26.48,
        "Product D": 12.54,
        "Product E": 14.86,
    }

    for product, expected_share in expected_market_shares.items():
        assert market_data.get(product) == pytest.approx(expected_share, rel=1e-2)

