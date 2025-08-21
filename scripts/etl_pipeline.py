import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt


#  ----- Extract -----
df = pd.read_csv("data/raw/BMW_Car_Sales_Classification.csv")

#  ----- Transform -----
df.dropna(inplace=True)  
df['Year'] = df['Year'].astype(int)  

df.to_csv("data/processed/cleaned_BMW_Car_Sales_Classification.csv", index=False)

# ----- Load -----

## 1. Connect to Postgres
engine = create_engine("postgresql://admin:admin@localhost:5432/bmw_sales")
df.to_sql('bmw_car_sales', engine, if_exists='replace', index=False)
print(" Data loaded into PostgreSQL!")
 
## 2. query the data

# Total sales per year
query1 = """
SELECT "Year", COUNT(*) AS total_sales
FROM bmw_car_sales
GROUP BY "Year"
ORDER BY "Year";
"""

df_sales = pd.read_sql(query1, engine)
print("\n Total Sales per Year:")
print(df_sales)

df_sales.plot(kind="bar", x="Year", y="total_sales", legend=False, title="Total Sales per Year")
plt.savefig("data/processed/sales_per_year.png")
plt.close()

#  Average selling price by model
query2 = """
SELECT "Model", AVG("Price_USD") AS avg_price
FROM bmw_car_sales
GROUP BY "Model"
ORDER BY avg_price DESC;
"""
df_price = pd.read_sql(query2, engine)
print("\n Average Selling Price by Model:")
print(df_price.head(10))

# Plot top 10
df_price.head(10).plot(kind="barh", x="Model", y="avg_price", legend=False, title="Top 10 Models by Avg Price")
plt.savefig("data/processed/avg_price_by_model.png")
plt.close()



