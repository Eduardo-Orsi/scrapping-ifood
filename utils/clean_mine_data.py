import pandas as pd
import sqlite3 as db
import requests, uuid
from datetime import datetime

TEST_DB = 'DB/teste.db'
MINING_DB = 'DB/iFood.db'
PRODUCTS_DB = 'DB/iFoodProducts.db'

def main():

    conn = db.connect(MINING_DB)
    cursor = conn.cursor()

    df_teste = pd.read_sql_query('SELECT * FROM raw_data;', conn)

    df = df_teste[:-1]
    df = df.drop(columns=['cep', 'cnpj'])
    df = df.dropna()

    # Faz a limpeza dos dados do scrapping
    df["price"] = df["price"].str.replace("R$ ", "", regex=False)
    df["price"] = df["price"].str.replace("A partir de ", "", regex=False)
    df = df.dropna()
    df["price"] = df["price"].str.extract("([0-9]{1,3},[0-9]{2})")
    df["price"] = df["price"].str.replace(",", ".")
    df["price"] = pd.to_numeric(df["price"])

    df["date_colected1"] = pd.to_datetime(df["date_colected"], format="%Y-%m-%d %H:%M:%S", errors='coerce')
    df["date_colected2"] = pd.to_datetime(df["date_colected"], format="%d/%m/%Y %H:%M", errors='coerce')
    df["date_colected"] = df["date_colected1"].combine_first(df["date_colected2"])
    df = df.drop(columns=["date_colected2", "date_colected1"])

    df["data"] = df["date_colected"].dt.strftime("%d/%m/%Y")
    df["date_colected"] = pd.to_datetime(df["date_colected"])
    date_list = ["18/01/2023", "28/01/2023", "30/01/2023", "31/01/2023"]

    for date in date_list:
        duplicated_df = df[df['data'] == date].copy()
        dt = pd.to_datetime(date, format="%d/%m/%Y")
        dt_plus_one_day = dt + pd.Timedelta(days=1)
        duplicated_df["date_colected"] = dt_plus_one_day
        duplicated_df["data"] = duplicated_df["date_colected"].dt.strftime("%d/%m/%Y")
        df = pd.concat([df, duplicated_df], ignore_index=True)

    df.sort_values(by="date_colected", inplace=True)
    df = df.drop(columns="date_colected")
    df.to_excel("scrapping_data_24_02.xlsx", index=False)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()