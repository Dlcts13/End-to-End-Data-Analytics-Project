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
