import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt


#  ----- Extract -----
df = pd.read_csv("../data/raw/BMW_Car_Sales_Classification.csv")

#  ----- Transform -----
df.dropna(inplace=True)  
df['Year'] = df['Year'].astype(int)  

df.to_csv("../data/processed/cleaned_BMW_Car_Sales_Classification.csv", index=False)
# ----- Load -----

## 1. Connect to Postgres
engine = create_engine("postgresql://admin:admin@localhost:5432/BMW_Car_Sales_Classification=#")
df.to_sql('bmw_car_sales', engine, if_exists='replace', index=False)
print(" Data loaded into PostgreSQL!")
 
## 2. query the data

# Total sales per year
query1 = """
SELECT "Year", COUNT(*) AS total_sales
FROM BMW_Car_Sales_Classification
GROUP BY "Year"
ORDER BY "Year";
"""

df_sales = pd.read_sql(query1, engine)
print("\n Total Sales per Year:")
print(df_sales)

df_sales.plot(kind ="")
df_sales.plot(kind="bar", x="Year", y="total_sales", legend=true, title="Total Sales per Year")
plt.savefig("../data/processed/sales_per_year.png")
plt.close()



