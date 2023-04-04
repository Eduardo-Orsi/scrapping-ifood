from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Union
import pandas as pd
import sqlite3 as db
import time, logging, uuid, requests, json
from query import ORDER_INFO_QUERY, ORDER_QUERY, ORDER_URL, ORDER_INFO_URL, HEADER_BASE

MINING_DB = 'DB/iFood.db'
TEST_DB = 'DB/orders.db'

RESTAURANT_ID = "60c626ee-4432-4ae4-84f6-352f04d00dfe"

class BotIFood:

    def __init__(self, db_name:str, need_selenium:bool = True):

        self.start_time = datetime.now()
        self.db_name = db_name
        self.db_conn = self.__conect_to_db()
        self.from_date = None
        self.to_date = None

        if need_selenium:
            options = webdriver.ChromeOptions()
            self.driver = webdriver.Chrome(options=options)
            self.driver.maximize_window()

    def mine_price_data(self, data_table_name:str) -> None:

        logging.basicConfig(filename=f'Logs/Log bot iFood - {self.start_time.strftime("%d.%m.%Y-%H.%M.%S")}.log', level=logging.INFO, format="%(asctime)s | %(message)s")
        logging.info(f'WEBSCRAPPING INCIADO: {self.start_time.strftime("%d.%m.%Y-%H.%M.%S")}')
        
        if not self.db_conn:
            return None

        urls = self.get_ifood_links()
        if urls.empty:
            return None

        for index, row in urls.iterrows():
            try:
                logging.info(f'SCRAPPING SITE: {row["Link iFood"]}')
                self.driver.get(row['Link iFood'])
                time.sleep(7)

                restaurant_name = self.driver.find_element(By.CLASS_NAME, 'merchant-info__title').text
                precos = self.driver.find_elements(By.CLASS_NAME, 'dish-card__price')
                descricaos = self.driver.find_elements(By.CLASS_NAME, 'dish-card__description')

                price_list = [preco.text for preco in precos]
                product_list = [descricao.text for descricao in descricaos]

                dicionario = {
                    "restaurant": restaurant_name,
                    "product": product_list,
                    "price": price_list,
                    "date_colected": self.start_time.strftime("%d/%m/%Y %H:%M")
                }

                df = pd.DataFrame(dicionario)
                self.df_to_sql(df=df, data_table_name=data_table_name)

                logging.info(f'SCRAPPING FINALIZADO SITE: {row["Link iFood"]}')

                self.clean_all_chrome_data()
            
            except Exception as es:
                logging.critical(f'PROBLEMA NO SCRAPPING DO SITE: {row["Link iFood"]} - {es}')
                self.clean_all_chrome_data()

        self.db_conn.commit()
        self.db_conn.close()
        logging.info(f'WEBSCRAPPING FINALIZADO!')
        logging.info(f'TEMPO DE EXECUÇÃO: {datetime.now() - self.start_time}')
        self.driver.close()

    def get_merchant_sales(self, token:str) -> None:
        self.to_date = datetime.today()
        self.from_date = self.to_date - relativedelta(months=1)
        while True:
            body = self.build_body(self.from_date, self.to_date, 150, False)
            HEADER_BASE["authorization"] = f"Bearer {token}"
            response = requests.post(url=ORDER_URL, headers=HEADER_BASE, data=body)
            if not response or response.status_code != 200:
                print(f"ERRO NO REQUEST {self.from_date} to {self.to_date}: {response.json()}")
                continue
            
            response = response.json()
            if not response["data"]["orders"]["groups"]:
                print(f"TERMINOU: {self.from_date} to {self.to_date}")
                break
            
            with open(f"json_order/json_response_{self.from_date.strftime('%Y-%m-%d')}_to_{self.to_date.strftime('%Y-%m-%d')}.json", "w") as f:
                json.dump(response, f)

            for group in response["data"]["orders"]["groups"]:
                self.extract_data(group)

            self.to_date -= relativedelta(months=1)
            self.from_date -= relativedelta(months=1)
        print("Terminou!")

    def extract_data(self, group:dict) -> None:

        day_sales = self.get_day_sales(group)
        self.save_day_sales(day_sales)

        for order in group["orders"]:
            body = self.build_body(self.from_date, self.to_date, 0, True, order["id"])
            order = requests.post(url=ORDER_INFO_URL, headers=HEADER_BASE, data=body)
            if not order or order.status_code != 200:
                print(f"ERRO NO REQUEST {self.from_date} to {self.to_date}: {order.json()}")
                continue
            order = order.json()
            if not order:
                print(order)
            order = order["data"]["order"]

            costumer = self.get_customer(order["customer"])
            self.save_customer(costumer)

            paymant_method = self.get_paymant_method(order["payments"]["methods"])
            paymant_method_id = self.save_paymant_method(paymant_method)

            delivery_method = self.get_delivery_method(order["deliveryMethod"])
            delivery_id = self.save_delivery_method(delivery_method)

            sale_channel = self.get_sale_channel(order["saleChannel"])
            sale_channel_id = self.save_sale_channel(sale_channel)

            order_info = self.get_order(order, RESTAURANT_ID, day_sales[0], costumer[0], paymant_method_id, delivery_id, sale_channel_id)
            order_id = self.save_order(order_info)

            if order["fees"]["total"]["value"] != 0.0:
                fees_list = self.get_fees(order["fees"], order_id)
                self.save_fees(fees_list)

            if order.get("benefits", None) != None:
                campaigns_list = self.get_campaign(order["benefits"], order_id)
                self.save_campaign(campaigns_list)

            items_subitems_list = self.get_items_and_subitems(order["customBag"]["items"], order_id)
            self.save_items(items_subitems_list[0])

            self.save_sub_items(items_subitems_list[1])
            print("\n")

    def save_sub_items(self, sub_items_list:list[tuple]) -> None:
        try:
            for sub_item in sub_items_list:
                c = self.__get_db_cursor()
                c.execute("INSERT INTO subitem (id_subitem, id_item, id_ifood_subitem, name, quantity, effective_unit_price, total_effective_unit_price, is_cancelled, cancelled_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", sub_item)
                c.close()
                self.db_conn.commit()
        except Exception as es:
            print(f"ERRO AO SALVAR SUB ITEMS: {es}")

    def get_sub_items(self, sub_items:list[dict], id_item:str) -> list[tuple]:
        try:
            sub_item_list = []
            for sub_item in sub_items:
                data = (
                    str(BotIFood.get_new_id()),
                    id_item,
                    sub_item["id"],
                    sub_item["name"],
                    sub_item["quantity"],
                    sub_item["effectiveUnitPrice"]["value"],
                    sub_item["totalEffectiveUnitPrice"]["value"],
                    int(sub_item["isCancelled"]),
                    sub_item["cancelledMessage"]
                )
                sub_item_list.append(data)
            return sub_item_list
        except Exception as es:
            print(f"ERRO AO PEGAR OS SUB ITEMS: {es}")

    def save_items(self, items_list:list[tuple]) -> None:
        try:
            for item in items_list:
                c = self.__get_db_cursor()
                c.execute("INSERT INTO item (id_item, id_order, id_ifood_item, name, quantity, note, original_unit_price, effective_unit_price, total_effective_unit_price, total_discount, is_cancelled, has_cancelled_sub_items, cancelled_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", item)
                c.close()
                self.db_conn.commit()
        except Exception as es:
            print(f"ERRO AO SALVAR OS ITEMS: {es}")

    def get_items_and_subitems(self, items:list[dict], id_order:str) -> list[tuple]:
        try:
            items_list = []
            sub_items_list = []
            for item in items:
                id = str(BotIFood.get_new_id())
                data = (
                    id,
                    id_order,
                    item["id"],
                    item["name"],
                    item["quantity"],
                    item["note"],
                    item["originalUnitPrice"]["value"],
                    item["effectiveUnitPrice"]["value"],
                    item["totalEffectiveUnitPrice"]["value"],
                    item["totalDiscount"]["value"],
                    int(item["isCancelled"]),
                    int(item["hasCancelledSubItems"]),
                    item["cancelledMessage"]
                )
                items_list.append(data)
                sub_items_list.extend(self.get_sub_items(item["subItems"], id))
            return [items_list, sub_items_list]
        except Exception as es:
            print(f"ERRO AO PEGAR OS ITEMS: {es}")

    def save_campaign(self, campaigns_list:list[tuple]) -> None:
        try:
            for campaign in campaigns_list:
                c = self.__get_db_cursor()
                c.execute("INSERT INTO cupom (id_cupom, id_order, name, amount, restaurant_sponsorship, ifood_sponsorship) VALUES (?, ?, ?, ?, ?, ?);", campaign)
                c.close()
                self.db_conn.commit()
        except Exception as es:
            print(f"ERRO AO SALVAR A CAMPANHA: {es}")

    def get_campaign(self, campaigns:dict, id_order:str) -> list[tuple]:
        try:
            campaigns_list = []
            for campaign in campaigns["details"]:
                own = campaign.get("sponsorshipValues", None)
                own = own.get("OWN", None)
                partner = campaign.get("sponsorshipValues", None)
                partner = partner.get("PARTNER", None)
                
                if own:
                    own = own.get("value", None)
                if partner:
                    partner = partner.get("value", None)

                data = (
                    str(BotIFood.get_new_id()),
                    id_order,
                    campaign["campaign"],
                    campaign["amount"]["value"],
                    partner,
                    own
                )
            campaigns_list.append(data)
            return campaigns_list
        except Exception as es:
            print(f"ERRO AO PEGAR A CAMPANHA: {es}")

    def save_fees(self, fees_list:list[tuple]) -> None:
        try:
            for fee in fees_list:
                c = self.__get_db_cursor()
                c.execute("INSERT INTO fees (id_fee, id_order, type, title, description, tooltip, value) VALUES (?, ?, ?, ?, ?, ?, ?);", fee)
                c.close()
                self.db_conn.commit()
        except Exception as es:
            print(f"ERRO AO SALVAR FEES: {es}")

    def get_fees(self, fees:dict, id_order:str) -> list[tuple]:
        try:
            fees_list = []
            for fee in fees["values"]:
                data = (
                    str(BotIFood.get_new_id()),
                    id_order,
                    fee["type"],
                    fee["title"],
                    fee["description"],
                    fee["tooltip"],
                    fee["amount"]["value"]
                )
                fees_list.append(data)
            return fees_list
        except Exception as es:
            print(f"ERRO AO PEGAR OS FEES: {es}")

    def save_order(self, order:tuple) -> str:
        try:
            c = self.__get_db_cursor()
            c.execute(
                """
                    INSERT INTO orders (id_order, id_restaurant, id_sale_day, short_id, last_status, id_customer, id_sale_channel, partial_price, total_price, discount, promotional_incentive, ifood_incentive, restaurant_incentive, total_value_cancelled_items, taxa_ifood, taxa_entrega, paid_price, id_paymant_method, id_delivery_method, is_cancellable, order_complete_date, order_date, order_hour, extra_info, status_json, events_json, financial_ocurrences_json, patch_committed_events_json) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                order
            )
            c.close()
            self.db_conn.commit()
            return order[0]
        except Exception as es:
            print(f"ERRO AO SALVAR O ORDER: {es}")

    def get_order(self, order:dict, restaurant_id:str, day_sales_id:str, costumer_id:str, paymant_id:str, delivery_id:str, channel_id:str) -> tuple:
        try:
            # print(order)
            benefits = order.get("benefits", None)
            if not benefits:
                total_benefits = 0.0
                own_benefits = 0.0
                partner_benefits = 0.0
            else:
                total_benefits = order["benefits"]["total"]["value"]

                own_benefits = benefits.get("own", None)
                if own_benefits:
                    own_benefits = own_benefits.get("value", None)

                partner_benefits = benefits.get("partner", None)
                if partner_benefits:
                    partner_benefits = partner_benefits.get("value", None)

            financial_ocurrences = order.get("financialOcurrences", None)
            if financial_ocurrences:
                financial_ocurrences = json.dumps(financial_ocurrences)

            if not financial_ocurrences:
                financial_ocurrences = None

            patch_committed_events = order.get("patchCommittedEvents", None)
            if patch_committed_events:
                patch_committed_events = json.dumps(patch_committed_events)

            if not patch_committed_events:
                patch_committed_events = None

            date_time = datetime.strptime(order["createdAt"], "%Y-%m-%dT%H:%M:%S.%f%z")

            data = (
                order["id"],
                restaurant_id,
                day_sales_id,
                order["shortId"],
                order["currentStatus"],
                costumer_id,
                channel_id,
                order["customBag"]["partial"]["value"],
                order["customBag"]["total"]["value"],
                order["customBag"]["discount"]['value'],
                total_benefits,
                own_benefits,
                partner_benefits,
                order["customBag"]["totalValueCancelledItems"]["value"],
                order["fees"]["total"]["value"],
                order["deliveryMethod"]["total"]["value"],
                order["payments"]["total"]["value"],
                paymant_id,
                delivery_id,
                int(order["isCancellable"]),
                order["createdAt"],
                date_time.strftime("%d/%m/%Y"),
                date_time.strftime("%H:%M:%S"),
                order["extraInfo"],
                json.dumps(order["status"]),
                json.dumps(order["events"]),
                financial_ocurrences,
                patch_committed_events
            ) 
            return data
        except Exception as es:
            print(f"ERRO AO PEGAR ORDER: {es}")
            return ()

    def save_sale_channel(self, sale_channel_data:tuple) -> str:
        try:
            c = self.__get_db_cursor()
            c.execute(f'SELECT * FROM sale_channel WHERE ifood_key = "{sale_channel_data[1]}";')
            result = c.fetchone()
            if not result:
                c.close()
                c = self.__get_db_cursor()
                c.execute("INSERT INTO sale_channel (id_sale_channel, ifood_key, value, label) VALUES (?, ?, ?, ?);", sale_channel_data)
                c.close()
                self.db_conn.commit()
                return sale_channel_data[0]
            c.close()
            return result[0]
        except Exception as es:
            print(f"ERRO AO SALAR O SALE CHANNEL: {es}")

    def get_sale_channel(self, sale_channel:dict) -> tuple:
        try:
            data = (
                str(BotIFood.get_new_id()),
                sale_channel["key"],
                sale_channel["value"],
                sale_channel["label"]
            )
            return data
        except Exception as es:
            print(f"ERRO AO PEGAR O SALE CHANNEL: {es}")
            return ()

    def save_delivery_method(self, delivery_method_data:tuple) -> str:
        try:
            c = self.__get_db_cursor()
            c.execute("INSERT INTO delivery_method (id_delivery_method, type, note, taxa_entrega, scheduling_type, data, scheduled_date, is_scheduled) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", delivery_method_data)
            c.close()
            self.db_conn.commit()
            return delivery_method_data[0]
        except Exception as es:
            print(f"ERRO AO SALVAR O DELIVERY METHOD: {es}")
            
    def get_delivery_method(self, delivery_method:dict) -> tuple:
        try:
            data = (
                str(BotIFood.get_new_id()),
                delivery_method["mode"]["type"],
                delivery_method["note"],
                delivery_method["total"]["value"],
                delivery_method["scheduling"]["type"],
                delivery_method["scheduling"]["date"],
                delivery_method["scheduling"]["scheduledDate"],
                int(delivery_method["scheduling"]["isScheduled"])
            )
            return data
        except Exception as es:
            print(f"ERRO AO PEGAR O DELIVERY METHOD: {es}")
            return ()

    def save_paymant_method(self, paymant_data:tuple) -> str:
        try:
            paymant_data = paymant_data[0]
            c = self.__get_db_cursor()
            c.execute("INSERT INTO paymant_methods (id_paymant_method, description, method, liability, type, brand) VALUES (?, ?, ?, ?, ?, ?);", paymant_data)
            c.close()
            self.db_conn.commit()
            return paymant_data[0]
        except Exception as es:
            print(f"ERRO AO SALVAR O PAYMANT METHOD: {es}")
            return paymant_data[0]

    def get_paymant_method(self, list_paymant_method:dict) -> list[tuple]:
        try:
            list_paymants_method = []
            for method in list_paymant_method:
                data = (
                    str(method["id"]).replace("-", ""),
                    method["description"],
                    method["method"],
                    method["liability"],
                    method["type"],
                    method["brand"]
                )
                list_paymants_method.append(data)
            return list_paymants_method
        except Exception as es:
            print(f"ERRO AO PEGAR O PAYMANT METHOD: {es}")
            return ()
            
    def save_customer(self, customer_data:tuple) -> None:
        try:
            c = self.__get_db_cursor()
            c.execute("INSERT INTO customer (id_customer, name, sams_club_account_status, sams_club_account_number, total_orders) VALUES (?, ?, ?, ?, ?);", customer_data)
            c.close()
            self.db_conn.commit()
        except Exception as es:
            print(f"ERRO AO SALVAR O CUSTOMER: {es}")

    def get_customer(self, customer:dict) -> tuple:
        club = customer.get("samsClubAccountStatus", None)
        data = (
            customer["id"],
            customer["name"],
            club,
            None,
            customer["totalOrders"]
        )
        return data

    def get_day_sales(self, group:dict) -> tuple:
        try:
            id = f"{str(uuid.uuid4())}"
            data = (
                id,
                group["date"],
                group["results"],
                group["totalDayValue"]["value"],
                group["totalDayValue"]["currency"]
            )
            return data
        except Exception as es:
            print(f"ERRO AO PEGAR DAY SALES: {self.from_date} to {self.to_date}")
            return ()

    def save_day_sales(self, day_sales_data:tuple) -> None:
        try:
            c = self.__get_db_cursor()
            c.execute("INSERT INTO day_sales (id_sale_day, sales_date, results, total_day_sales, currency) VALUES (?, ?, ?, ?, ?);", day_sales_data)
            c.close()
            self.db_conn.commit()
        except Exception as es:
            print(f"ERRO AO SALVAR O DAY SALES: {es}")

    def build_body(self, from_date:datetime, to_date:datetime, size:int, is_order_info:bool, order_id:str = "") -> str:
        body = {}

        if is_order_info:
            body["query"] = ORDER_INFO_QUERY
            body["variables"] = {}
            body["variables"]["orderId"] = order_id
            body = json.dumps(body)
            return body
        
        body["query"] = ORDER_QUERY
        body["variables"] = {}
        body["variables"]["from"] = from_date.strftime("%Y-%m-%d")
        body["variables"]["to"] = to_date.strftime("%Y-%m-%d")
        body["variables"]["storeIds"] = ["60c626ee-4432-4ae4-84f6-352f04d00dfe"]
        body["variables"]["page"] = 0
        body["variables"]["size"] = size
        body["variables"]["includeStore"] = False
        body = json.dumps(body)
        return body

    def clean_all_chrome_data(self) -> None:
        self.driver.delete_all_cookies()
        self.driver.execute_script('window.localStorage.clear();')
        self.driver.execute_script('window.sessionStorage.clear();')

    def get_ifood_links(self) -> Union[pd.DataFrame, None]:
        try:
            df_links = pd.read_csv('src/links.csv')
            return df_links
        except Exception as es:
            logging.critical(f'NÃO FOI POSSÍVEL ABRIR O EXCEL DE LINKS: {es}')
            return None

    def __conect_to_db(self) -> Union[db.Connection, None]:
        try:
            conn = db.connect(self.db_name)
            return conn
        except Exception as es:
            logging.critical(f'NÃO FOI POSSÍVEL CONECTAR COM O BANCO DE DADOS: {es}')
            return None

    def __get_db_cursor(self) -> Union[db.Cursor, None]:
        try:
            cursor = self.db_conn.cursor()
            return cursor
        except Exception as es:
            logging.critical(f'NÃO FOI POSSÍVEL PEGAR O CURSOR DO BANCO DE DADOS: {es}')
            return None

    def df_to_sql(self, df:pd.DataFrame, data_table_name:str) -> bool:
        try:
            df.to_sql(data_table_name, self.db_conn, if_exists="append", index=False)
            self.db_conn.commit()
            return True
        except Exception as es:
            logging.critical(f'NÃO FOI POSSÍVEL SALVAR NO BANCO DE DADOS: {es}')
            return False

    @staticmethod
    def get_new_id() -> str:
        return str(uuid.uuid4())

if __name__ == "__main__":

    bot = BotIFood(TEST_DB, need_selenium=True)
    bot.mine_price_data(data_table_name="raw_data")

    # bot = BotIFood(TEST_DB, need_selenium=False)
    # bot.get_merchant_sales(token="Seu token vai aqui")

