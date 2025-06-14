import requests
import pandas as pd
import sqlite3


# 1 - extracting data or read data
# 1-1 - reading from url
def extract():
    """
    Extract data from a provided url
    Returns a pandas DataFrame.
    """
    print("Starting data extraction...")
    url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    df = pd.read_csv(url)
    return df

# 1-2- or read from downloaded csv
# df = pd.read_csv("owid-covid-data.csv")
# # print (df.head())

# 2 - transforming data
def transform(df):
    """
    Transform data: cleaning and parsing
    """
    print("Starting data transformation...")
    # 2-1- Clean Columns
    df = df[['location', 'date', 'total_cases', 'new_cases']]
    df.dropna(inplace=True)
    df = df[df['new_cases']>0]

    # 2-2- Parse Data Types
    df['date'] = pd.to_datetime(df['date'])
    df['total_cases'] = df['total_cases'].astype(int)
    df['new_cases'] = df['new_cases'].astype(int)
    print(f"Transformed data has {len(df)} rows.")
    return df


def load(df):
    """
    Load the transformed data into a local SQLite database.
    """
    print("Starting data loading...")
    conn = sqlite3.connect("covid_data.db")
    df.to_sql("covid_stats", conn, if_exists='replace', index=False)
    conn.close()
    print("Data loaded into SQLite database successfully.")

if __name__ == "__main__":
    df = extract()
    df_transformed = transform(df)
    load(df_transformed)
