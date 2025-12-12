import pandas as pd
import numpy as np
import sqlalchemy
from db_connect import get_db_connection

#Connections
df = pd.read_csv("C:\\Users\\Darius Lacatusu\\Desktop\\End-to-End-Data-Analytics-Project\\res\\customer_shopping_behavior.csv")

print(df.head())
print("--------------------")
print(df.info())
print("--------------------")
print(df.describe())
print("--------------------")
print(df.describe(include='all'))
print("--------------------")
print(df.isnull().sum())
print("--------------------")

#Handling missing values
df["Review Rating"]=df.groupby("Category")["Review Rating"].transform(lambda x: x.fillna(x.median()))
print(df.isnull().sum())
print("--------------------")
df.columns=df.columns.str.lower()
df.columns=df.columns.str.replace(" ", "_")
df=df.rename(columns={"purchase_amount_(usd)": "purchase_amount"})
print(df.info())

#Create a column age group 
labels= ["Young Adult", "Adult", "Middle-aged", "Senior"]
df["age_group"]=pd.qcut(df["age"],q=4,labels=labels)
print(df[["age", "age_group"]].head(5))

# Create column purchase_frequency_days
print(df.columns)

frequency_mapping={
    "Weekly": 7,
    "Monthly": 30,
    "Quarterly": 90,
    "Bi-Weekly": 14,
    "Annually": 365,
    "Every 3 Months": 90,
    "Fortnightly": 14
}

df["purchase_frequency_days"]=df["frequency_of_purchases"].map(frequency_mapping)
print(df[["purchase_frequency_days","frequency_of_purchases"]].head(20))
print(df["frequency_of_purchases"].unique())

#Drop useless column
print(df[["discount_applied","promo_code_used"]])
print((df["discount_applied"]==df["promo_code_used"]).all())
df= df.drop(columns="promo_code_used")
print(df.head(5))
print(df.columns)

# Database: connect and load dataframe into the `customer` table
conn = get_db_connection()
table_name = "customer"

# Load DataFrame into SQL Server customer table
if conn is None:
    print("No database connection available - aborting load.")
else:
    cursor = conn.cursor()

    # Create table if not exists (SQL Server compatible)
    create_table_sql = f"""
IF OBJECT_ID('dbo.{table_name}', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.{table_name} (
        customer_id INT PRIMARY KEY,
        age INT,
        gender NVARCHAR(20),
        item_purchased NVARCHAR(255),
        category NVARCHAR(100),
        purchase_amount FLOAT,
        location NVARCHAR(100),
        size NVARCHAR(20),
        color NVARCHAR(50),
        season NVARCHAR(50),
        review_rating FLOAT,
        subscription_status NVARCHAR(10),
        shipping_type NVARCHAR(50),
        discount_applied NVARCHAR(10),
        previous_purchases INT,
        payment_method NVARCHAR(50),
        frequency_of_purchases NVARCHAR(50),
        age_group NVARCHAR(50),
        purchase_frequency_days INT
    )
END
"""

    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{table_name}' is ready.")
    except Exception as e:
        print("Error creating table:", e)

    # Optional: clear table before loading to avoid duplicates
    try:
        cursor.execute(f"IF OBJECT_ID('dbo.{table_name}', 'U') IS NOT NULL BEGIN DELETE FROM dbo.{table_name} END")
        conn.commit()
        print(f"Existing rows in '{table_name}' cleared.")
    except Exception:
        pass

    # Prepare columns & insert
    columns = [
        "customer_id",
        "age",
        "gender",
        "item_purchased",
        "category",
        "purchase_amount",
        "location",
        "size",
        "color",
        "season",
        "review_rating",
        "subscription_status",
        "shipping_type",
        "discount_applied",
        "previous_purchases",
        "payment_method",
        "frequency_of_purchases",
        "age_group",
        "purchase_frequency_days",
    ]

    placeholders = ",".join(["?" for _ in columns])
    insert_sql = f"INSERT INTO dbo.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    # Build rows, converting NaNs to None
    rows = []
    for row in df[columns].itertuples(index=False, name=None):
        clean_row = tuple((None if (isinstance(v, float) and np.isnan(v)) else v) for v in row)
        rows.append(clean_row)

    # Bulk insert using pyodbc
    try:
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, rows)
        conn.commit()
        print(f"Inserted {len(rows)} rows into '{table_name}'.")
    except Exception as e:
        print("Error inserting rows:", e)
    finally:
        cursor.close()
        conn.close()


