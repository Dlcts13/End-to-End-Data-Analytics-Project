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
# Call the connection function (was assigned as the function object previously)
conn = get_db_connection()
table_name = "customer"

if conn is None:
    print("No DB connection available; skipping DB load.")
else:
    try:
        # Prefer using SQLAlchemy engine for df.to_sql
        import sqlalchemy
        from urllib.parse import quote_plus

        server = r"localhost\SQLEXPRESS"
        database = "customer_behavior"
        odbc_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
        )

        connect_url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_str)
        engine = sqlalchemy.create_engine(connect_url, fast_executemany=True)

        # Write dataframe to the database (replace the table if it exists)
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        print(f"Data written to table '{table_name}' (if_exists='replace').")

        # Read back a few rows to verify
        df_db = pd.read_sql_query(f"SELECT TOP 5 * FROM {table_name}", con=engine)
        print(df_db.head())

    except ModuleNotFoundError as e:
        print("Required package missing:", e)
        print("Install with: .venv\\Scripts\\python -m pip install sqlalchemy pyodbc")
    except Exception as e:
        print("Error loading data into DB:", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass
