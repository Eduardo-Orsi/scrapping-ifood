PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS restaurant (
					id_restaurant TEXT PRIMARY KEY NOT NULL,
					cnpj TEXT NOT NULL,
					cep TEXT,
					name TEXT,
					logradouro TEXT,
					complemento TEXT,
					cidade TEXT,
					bairro TEXT,
					uf TEXT,
					ddd TEXT);
					
CREATE TABLE IF NOT EXISTS day_sales (
					id_sale_day TEXT PRIMARY KEY NOT NULL,
					sales_date TEXT,
					results INTEGER,
					total_day_sales REAL,
					currency TEXT);

CREATE TABLE IF NOT EXISTS user(
					id_user TEXT PRIMARY KEY NOT NULL,
					id_restaurant TEXT,
					email TEXT,
					phone TEXT,
					FOREIGN KEY(id_restaurant) REFERENCES restaurant(id_restaurant));

CREATE TABLE IF NOT EXISTS customer( 
						id_customer TEXT PRIMARY KEY NOT NULL,
						name TEXT,
						sams_club_account_status TEXT,
						sams_club_account_number TEXT,
						total_orders INT);
						
CREATE TABLE IF NOT EXISTS paymant_methods (
					id_paymant_method TEXT PRIMARY KEY NOT NULL,
					description TEXT,
					method TEXT,
					liability TEXT,
					type TEXT,
					brand TEXT);
					
CREATE TAbLE IF NOT EXISTS delivery_method (
					id_delivery_method TEXT PRIMARY KEY NOT NULL,
					type TEXT,
					note TEXT,
					taxa_entrega REAL,
					scheduling_type TEXT,
					data TEXT,
					scheduled_date TEXT,
					is_scheduled INTEGER);
					
CREATE TABLE IF NOT EXISTS sale_channel (
				id_sale_channel TEXT PRIMARY KEY NOT NULL,
				ifood_key TEXT,
				value TEXT,
				label TEXT);
						
CREATE TABLE IF NOT EXISTS orders (
                        id_order TEXT PRIMARY KEY NOT NULL,
                        id_restaurant TEXT NOT NULL,
						id_sale_day TEXT,
						short_id TEXT,
						last_status TEXT,
						id_customer TEXT,
						id_sale_channel TEXT,
						partial_price REAL,
                        total_price REAL,
						discount REAL,
						promotional_incentive REAL,
						ifood_incentive REAL,
						restaurant_incentive REAL,
						total_value_cancelled_items REAL,
						taxa_ifood REAL,
						taxa_entrega REAL,
						paid_price REAL,
                        id_paymant_method TEXT,
						id_delivery_method TEXT,
						is_cancellable INTEGER,
                        order_complete_date TEXT,
						order_date TEXT,
						order_hour TEXT,
						extra_info TEXT,
						status_json TEXT,
						events_json TEXT,
						financial_ocurrences_json TEXT,
						patch_committed_events_json TEXT,
                        FOREIGN KEY(id_restaurant) REFERENCES restaurant(id_restaurant),
						FOREIGN KEY (id_sale_day) REFERENCES day_sales(id_sale_day),
						FOREIGN KEY(id_customer) REFERENCES costomer(id_customer),
						FOREIGN KEY(id_paymant_method) REFERENCES paymant_methods(id_paymant_method)
						FOREIGN KEY(id_delivery_method) REFERENCES delivery_method(id_delivery_method)
						FOREIGN KEY(id_sale_channel) REFERENCES sale_channel(id_sale_channel));
						
CREATE TABLE IF NOT EXISTS fees (
				id_fee  TEXT PRIMARY KEY NOT NULL,
				id_order TEXT,
				type TEXT,
				title TEXT,
				description TEXT,
				tooltip TEXT,
				value REAL,
				FOREIGN KEY (id_order) REFERENCES orders(id_order));
				
CREATE TABLE IF NOT EXISTS cupom (
				id_cupom TEXT PRIMARY KEY NOT NULL,
				id_order TEXT,
				name TEXT,
				amount REAL,
				restaurant_sponsorship REAL,
				ifood_sponsorship REAL,
				FOREIGN KEY (id_order) REFERENCES orders(id_order));
				
						
CREATE TABLE IF NOT EXISTS item (
						id_item TEXT PRIMARY KEY NOT NULL,
						id_order TEXT,
						id_ifood_item TEXT,
						name TEXT,
						quantity INTEGER,
						note TEXT,
						original_unit_price REAL,
						effective_unit_price REAL,
						total_effective_unit_price REAL,
						total_discount REAL,
						is_cancelled INTEGER,
						has_cancelled_sub_items INTEGER,
						cancelled_message TEXT,
						FOREIGN KEY(id_order) REFERENCES orders(id_order));
			
CREATE TABLE IF NOT EXISTS subitem (
					id_subitem TEXT PRIMARY KEY NOT NULL,
					id_item TEXT,
					id_ifood_subitem TEXT,
					name TEXT,
					quantity INTEGER,
					effective_unit_price REAL,
					total_effective_unit_price REAL,
					is_cancelled INTEGER,
					cancelled_message TEXT,
					FOREIGN KEY(id_item) REFERENCES item(id_item));
					