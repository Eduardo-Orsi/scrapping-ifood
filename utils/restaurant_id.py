import sqlite3 as db
import pandas as pd
from urllib.parse import urlparse

def get_url_path(url:str) -> str:
    parsed_url = urlparse(url)
    return parsed_url.path

def get_data_from_path(path:str) -> str:
    parts = path.split("/")
    city_state = parts[2].split("-")
    state = city_state[-1].upper()
    city_state.pop(-1)
    if len(city_state) > 1:
        city = " ".join(city_state).title()
    else:
        city = city_state[0].title()
    restaurant_name = parts[3].title().split("-")
    restaurant_name = " ".join(restaurant_name)
    id_restaurant = parts[4]
    return restaurant_name, city, state, id_restaurant 

df = pd.read_excel("/Users/eduardoayrpereiraorsi/code/bot_ifood/links.xlsx")
df["path"] = df["Link iFood"].apply(get_url_path)
df[["restaurant_name", "city", "state", "id_restaurant"]] = df["path"].apply(lambda x: pd.Series(get_data_from_path(x)))
df.drop(columns="path", inplace=True)
df.to_excel("links.xlsx", index=False)
