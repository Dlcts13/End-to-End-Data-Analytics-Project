import pandas as pd
import numpy as np

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