import sys
from pyspark.sql import SparkSession
from main.job.pipeline import SalesAnalysis

def main():
    # Task 1: Create SparkSession
    spark = SparkSession.builder.appName("Sales Data Analysis").getOrCreate()

    # Initialize the job
    job = SalesAnalysis(spark)

    # Task 2: Load data from CSV (path passed as command-line argument)
    data_path = sys.argv[1]
    df = job.load_data(data_path)

    # Task 3: Display schema
    print("<<Schema>>")
    job.display_schema(df)

    # Task 4: Filter for Germany
    print("<<Sales in Germany>>")
    job.filter_by_country(df, "Germany").show()

    # Task 5: Count total products
    print("<<Total Unique Products>>")
    print(job.total_products(df))

    # Task 6: Top 3 products by sales
    print("<<Top 3 Products>>")
    job.top_n_products(df, 3).show()

    # Task 7: Total Sales
    print("<<Total Sales>>")
    print(job.total_sales(df))

    # Task 8: Market Share by Product
    print("<<Market Share>>")
    job.market_share(df).show()

    spark.stop()

if __name__ == "__main__":
    main()
