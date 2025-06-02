## **Case Study: Product Sales Data Analysis using PySpark**

### **Context:**

As a **Data Engineer** at a multinational retail corporation, you are tasked with analyzing product sales across various regions. The data is stored in a CSV file and includes key details such as product IDs, product names, countries, and sales figures.

Your goal is to build a PySpark application that loads the dataset, performs analysis, and extracts business insights to assist in product performance evaluation and strategic decision-making.

---

### **Sample Data (products.csv):**

| product_id | product   | country | sales |
| ----------- | --------- | ------- | ----- |
| 1           | Product A | USA     | 100   |
| 2           | Product B | Canada  | 80    |
| 3           | Product C | Germany | 90    |
| 4           | Product D | UK      | 50    |
| 1           | Product A | Germany | 70    |

---

### **Your Tasks:**

Implement the following methods inside `pipeline.py`:

1. **init_spark_session(self) → SparkSession**
   Create a PySpark `SparkSession` with app name `"Sales Data Analysis"`.

2. **load_data(self, path: str) → DataFrame**
   Load the CSV file from the given local path into a PySpark DataFrame.

3. **display_schema(self, df: DataFrame) → None**
   Print the schema of the DataFrame to understand the data structure.

4. **filter_by_country(self, df: DataFrame, country: str = "Germany") → DataFrame**
   Return only those rows where the country matches `"Germany"`.

5. **total_products(self, df: DataFrame) → int**
   Find the total number of unique products based on `product_id`.

6. **top_n_products(self, df: DataFrame, n: int) → DataFrame**
   Identify the top `n` products by total sales.

7. **total_sales(self, df: DataFrame) → int**
   Calculate the total number of sales across all products.

8. **market_share(self, df: DataFrame) → DataFrame**
   Compute the market share (as a percentage) of each product based on total sales.


## Commands
- run: 
```bash
source venv/bin/activate; python3 src/app.py data/products.csv 
```
- install: 
```bash
bash make.sh __install; source venv/bin/activate; pip3 install -r requirements.txt
```
- test: 
```bash
source venv/bin/activate; py.test -p no:warnings
```