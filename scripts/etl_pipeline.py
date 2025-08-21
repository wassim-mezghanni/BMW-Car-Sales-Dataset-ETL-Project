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

# Success rate by region
query3 = """
SELECT "Region", AVG(CASE WHEN "Sales_Volume" > 4500 THEN 1 ELSE 0 END) AS success_rate
FROM bmw_car_sales
GROUP BY "Region"
ORDER BY success_rate DESC;
"""
df_success = pd.read_sql(query3, engine)
print("\n Success Rate by Region:")
print(df_success)

# Plot
df_success.plot(kind="bar", x="Region", y="success_rate", legend=False, title="Success Rate by Region")
plt.savefig("data/processed/success_rate_by_region.png")
plt.close()
# ----- Success Rate by Region (using classification) -----
query_success_class = """
SELECT "Region",
       COUNT(*) AS total_rows,
       AVG(CASE WHEN "Sales_Classification" = 'High' THEN 1 ELSE 0 END)::float AS success_rate
FROM bmw_car_sales
GROUP BY "Region"
ORDER BY success_rate DESC;
"""

df_success_class = pd.read_sql(query_success_class, engine)
print("\nSuccess Rate by Region (High classification proportion):")
print(df_success_class)

ax = df_success_class.plot(kind='bar', x='Region', y='success_rate', legend=False,
                           title='Success Rate by Region (Classification)')
ax.set_ylabel('Success Rate (%)')
ax.set_ylim(0,1)
for p in ax.patches:
    ax.annotate(f"{p.get_height()*100:.1f}%", (p.get_x()+p.get_width()/2, p.get_height()),
                ha='center', va='bottom', fontsize=7, rotation=0)
plt.tight_layout()
plt.savefig("data/processed/success_rate_by_region_class.png")
plt.close()

# ----- Success Rate by Region (dynamic 75th percentile volume threshold) -----
volume_threshold = float(df['Sales_Volume'].quantile(0.75))
query_success_dyn = f"""
SELECT "Region",
       COUNT(*) AS total_rows,
       AVG(CASE WHEN "Sales_Volume" > {volume_threshold} THEN 1 ELSE 0 END)::float AS success_rate
FROM bmw_car_sales
GROUP BY "Region"
ORDER BY success_rate DESC;
"""

df_success_dyn = pd.read_sql(query_success_dyn, engine)
print(f"\nSuccess Rate by Region (Sales_Volume > 75th pct = {volume_threshold:.0f}):")
print(df_success_dyn)

ax2 = df_success_dyn.plot(kind='bar', x='Region', y='success_rate', legend=False,
                          title='Success Rate by Region (Dynamic Threshold)')
ax2.set_ylabel('Success Rate (%)')
ax2.set_ylim(0,1)
for p in ax2.patches:
    ax2.annotate(f"{p.get_height()*100:.1f}%", (p.get_x()+p.get_width()/2, p.get_height()),
                 ha='center', va='bottom', fontsize=7)
plt.tight_layout()
plt.savefig("data/processed/success_rate_by_region_dynamic.png")
plt.close()

print("\n ETL + Analysis Completed! Results saved in /data/processed/")


