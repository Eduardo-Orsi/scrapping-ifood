import pandas as pd
import sqlite3 as db
import requests, uuid

TEST_DB = 'DB/teste.db'
PROD_DB = 'DB/iFood.db'
PRODUCTS_DB = 'DB/iFoodProducts.db'

def main():

    conn = db.connect(PROD_DB)
    cursor = conn.cursor()

    df_teste = pd.read_sql_query('SELECT * FROM raw_data;', conn)
    
    # Faz a tabela de Restaurates com o CEP e CNPJ
    df = df_teste[:-1]
    df = df.drop(columns=["price", "product"])
    df = df.dropna()
    df = df.drop(columns="date_colected")
    df = df.dropna(subset=["cep", "cnpj"])
    df = df.drop_duplicates(subset="restaurant")
    df["cep"] = df["cep"].apply(lambda x: x.strip())
    df["cnpj"] = df["cnpj"].apply(lambda x: x.strip())
    df["ID"] = [uuid.uuid4() for _ in range(len(df.index))]
    df[["logradouro", "complemento", "bairro", "cidade", "uf", "ddd"]] = None
    for index, row in df.iterrows():
        headers = {'user-agent': 'my-app/0.0.1'}
        response = requests.get(f"https://viacep.com.br/ws/{row['cep']}/json/", headers=headers).json()
        row["logradouro"] = response["logradouro"]
        row["complemento"] = response["complemento"]
        row["bairro"] = response["bairro"]
        row["uf"] = response["uf"]
        row["ddd"] = response["ddd"]
        print(row)
    df.to_excel("empresas.xlsx", index=False)

    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
