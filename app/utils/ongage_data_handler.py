import requests
from dotenv import load_dotenv
import os
import json
from tqdm import tqdm 
load_dotenv()

class OngageDataHandler:
    def __init__(self):
        self.url = f"https://api.ongage.net/{os.getenv('LIST_ID')}/api/v2/contacts/"
        self.header = {
            "X_USERNAME": os.getenv("X_USERNAME"),
            "X_PASSWORD": os.getenv("X_PASSWORD"),
            "X_ACCOUNT_CODE": os.getenv("X_ACCOUNT_CODE")
        }
        self.data_to_send = []
        self.country_codes = json.load(open('app/utils/country_codes.json', 'r'))
    
    def prepare_data(self, data):
        unsuccesful_emails = []
        for record in tqdm(data):
            country_code = self.country_codes.get(record[5].split(' - ')[1], "DEF")
            conversion_type = "REG" if record[9] != "FTD" else "FTD"
            generated_email = '+' + record[14] + '@yourmobile.com' if record[14] and record[14].strip() and record[14].strip().lower() != '<na>' else None
            if generated_email is None or "phone" in generated_email:
                unsuccesful_emails.append({
                    "click_id": record[7],
                    "conversion_date": record[39]
                })
                continue
            if country_code == 'GB':
                self.update_list(generated_email, conversion_type, country_code, record[5].split(' - ')[1], country_code + "_" + conversion_type, record)
        return unsuccesful_emails

    def update_list(self, generated_email, conversion_type, country_code, country, source, data):
        self.data_to_send.append({
            "email": generated_email,
            "overwrite": True,
            "fields": {
                "mobile": data[14],
                "country": country,
                "ip": data[29],
                "language": data[33],
                "source": source,
                "conversion_type": conversion_type,
                "is_ftd": "true" if conversion_type == "FTD" else "false",
                "click_id": data[7],
                "campaign_name": data[5],
                "campaign_id": data[4],
                "offer_name": data[35],
                "lander_name": data[32],
                "traffic_source_id": data[44],
                "traffic_source_name": data[45],
                "transaction_id": data[46],
                "device_type": data[24],
                "os": data[36],
                "browser": data[2],
                "revenue": data[43] if conversion_type == "FTD" else "",
                "currency": "",
                "external_id": data[26],
                "custom_var_1": data[14],
                "custom_var_2": data[16],
                "custom_var_3": data[17],
                "offer_id": data[34],
                "country_code": country_code,
                "conversion_date": data[39],
                "registration_date": data[39] if not conversion_type == "FTD" else "",
                "ftd_date": data[39] if conversion_type == "FTD" else "",
                "visit_timestamp": data[48],
            }
        })
    
    def clear_list(self):
        self.data_to_send = []
    
    def sync_list(self):
        data_parts, statuses = [], []
        for i in range(0, len(self.data_to_send), 500):
            rows_to_send = self.data_to_send[i: i+500]
            response = requests.post(
                url=self.url, headers=self.header, json=rows_to_send
            )
            if response.status_code == 200:
                print(f"Inserted {len(rows_to_send)} rows in ongage")
            else:
                print(f"Failed to insert {len(rows_to_send)} rows in ongage.")
            data_parts.append(self.data_to_send)
            statuses.append(response.status_code==200)
        self.clear_list()
        return data_parts, statuses
