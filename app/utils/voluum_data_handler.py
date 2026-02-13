import pandas as pd
import requests
import os
from dotenv import load_dotenv
from app.utils.encryption_handler import EncryptionHandler

load_dotenv()

encryption_handler = EncryptionHandler()

class VoluumDataHandler:
    def __init__(self):
        pass

    def classifyRecord(self, osVersion):
        [os, version] = osVersion.split(' ') if osVersion.count(' ') == 1 else [osVersion, 'nil']
        majorVersion = int(version.split('.')[0]) if version != 'nil' and version[0].isdigit() else 9999
        # BLACKLIST rules first
        if os == 'IOS' and majorVersion <= 10:
            return 'BLACKLIST'   
        if os == 'Android' and majorVersion <= 7:
            return 'BLACKLIST'
        if osVersion in ['Windows 7', 'Windows XP'] or 'Ubuntu' in osVersion:
            return 'BLACKLIST'

        # MONITOR rules
        if os == 'Android' and majorVersion in [8, 9, 9999]:
            return 'MONITOR'
        if osVersion in ['Windows 10', 'Windows 11'] or os == 'Linux':
            return 'MONITOR'
        if os == 'Android' and 17 <= majorVersion:
            return "MONITOR"

        # WHITELIST rules
        if os == 'IOS' and 11 <= majorVersion:
            return 'WHITELIST'
        if 'MacOS' in os or 'Mac' in os:
            return 'WHITELIST'
        if os == 'Android' and 10 <= majorVersion < 17:
            return 'WHITELIST'  
        return 'UNKNOWN'
    
    def sort_data(self):
        self.data["category"] = self.data["osVersion"].apply(self.classifyRecord)
    
    def clean_unique_visits(self):
        # 1. Sort so valid rows come first
        self.data.sort_values(by="uniqueVisits", ascending=False, inplace=True)

        # 2. Split dataframe
        valid_visits = self.data[self.data["uniqueVisits"] == 1]
        zero_visits = self.data[self.data["uniqueVisits"] == 0]

        # 3. Remove duplicates ONLY from zero-visit rows
        zero_visits_cleaned = zero_visits.drop_duplicates(
            subset="customVariable1",
            keep="first"
        )

        # 4. Merge back
        final_data = pd.concat(
            [valid_visits, zero_visits_cleaned],
            ignore_index=True
        )

        # 5. Remove any remaining duplicates (should be none)
        final_data = final_data.drop_duplicates(
            subset="customVariable1",
            keep="first"
        )

        self.data = final_data

    def create_session_token(self):
        url = "https://api.voluum.com/auth/access/session"

        payload = {
            "accessId": os.getenv("VOLUUM_ACCESS_ID"),
            "accessKey": os.getenv("VOLUUM_ACCESS_KEY")
        }

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)

        return response.json()["token"]

    def get_traffic_source_data(self, traffic_source_id, limit="1000", from_date="2025-12-06T00:00:00.000Z", to_date="2025-12-13T00:00:00.000Z"):
        access_token = self.create_session_token()

        url = f"https://panel-api2.voluum.com/report?reportType=table&limit={limit}&dateRange=custom-date-time&from={from_date}&to={to_date}&searchMode=TEXT&currency=EUR&sort=visits&direction=ASC&reportDataType=0&offset=OFFSET_VALUE&groupBy=custom-variable-1&groupBy=ip&groupBy=browser-version&groupBy=os-version&column=profit&column=customVariable1&column=visits&column=uniqueVisits&column=suspiciousVisitsPercentage&column=campaignNoConversionsWarning&column=conversions&column=costSources&column=cost&column=revenue&column=roi&column=cv&column=epv&column=cpv&column=errors&column=Click2Reg&column=Reg2FTD&column=customConversions1&column=customRevenue1&column=customConversions2&column=customRevenue2&column=customConversions3&column=customRevenue3&column=osVersion&column=browserVersion&column=actions&column=type&column=clicks&column=suspiciousClicksPercentage&column=suspiciousVisits&column=suspiciousClicks&tz=Etc/GMT&filter1=traffic-source&filter1Value={traffic_source_id}"
        
        headers = {
            "cwauth-token": access_token
        }

        total_data = []
        offset = 0
        while True:
            paginated_url = url.replace("OFFSET_VALUE", str(offset))
            response = requests.get(paginated_url, headers=headers)
            data = response.json()
            total_data.extend(data['rows'])
            print(f"Fetched {len(data['rows'])} rows at offset {offset} out of {data['totalRows']} total rows. Current total: {len(total_data)}")
            if len(total_data) == int(data["totalRows"]):
                break
            offset += int(limit)
        return total_data

    def get_data(self, limit="1000", from_date="2025-12-06T00:00:00.000Z", to_date="2025-12-13T00:00:00.000Z"):
        traffic_source_ids = ["61a5a37e-cf24-46cd-8a2f-038fd9c8d5f8", "4b62c9a1-6c3e-434b-aa7a-fbdf721f7e89"]
        # traffic_source_ids = ["4b62c9a1-6c3e-434b-aa7a-fbdf721f7e89"]
        total_data = []
        for traffic_source in traffic_source_ids:
            total_data.extend(self.get_traffic_source_data(traffic_source, limit, from_date, to_date))
        
        return total_data
    
    def get_data_as_dataframe(self, limit="10000", from_date="2025-12-12T00:00:00.000Z", to_date="2025-12-13T00:00:00.000Z"):
        data = self.get_data(limit, from_date, to_date)
        df = pd.DataFrame(data)
        df["customVariable1"] = df["customVariable1"].apply(self.decrypt_number)
        return df

    def remove_wrong_phone_numbers(self):
        # Phonenumber which are empty or {{phoneNumber}} are removed
        self.data['customVariable1'] = pd.to_numeric(self.data['customVariable1'], errors='coerce')
        self.data = self.data.dropna(subset=['customVariable1'])
        self.data = self.data[
            (self.data['customVariable1'] != '') & 
            (self.data['customVariable1'].notnull())
        ]

    def get_cleaned_data(self, from_date="2025-12-12T00:00:00.000Z", to_date="2025-12-13T00:00:00.000Z"):
        self.data = self.get_data_as_dataframe(from_date=from_date, to_date=to_date)
        self.remove_wrong_phone_numbers()
        self.sort_data()
        self.clean_unique_visits()
        return self.data
    
    def get_test_data_as_dataframe(self, file_path):
        df = pd.read_csv(file_path)
        return df
    
    def get_cleaned_test_data(self, file_path):
        self.data = self.get_test_data_as_dataframe(file_path)
        self.remove_wrong_phone_numbers()
        self.sort_data()
        self.clean_unique_visits()
        return self.data
    
    def sync_data_for_range(self):
        from_date = 1
        to_date = 14
        date_format = "2025-12-{day:02d}T00:00:00.000Z"
        for day in range(from_date, to_date):
            from_date_str = date_format.format(day=day)
            to_date_str = date_format.format(day=day+1)
            print(f"Syncing data from {from_date_str} to {to_date_str}")
            self.get_cleaned_data(from_date=from_date_str, to_date=to_date_str)

    def get_clean_data_from_csv(self, file_path):
        self.data = pd.read_csv(file_path)
        self.remove_wrong_phone_numbers()
        return self.data
    
    def get_conversions_data(self, limit="10000", from_date="2025-12-06T00:00:00.000Z", to_date="2025-12-13T00:00:00.000Z"):
        url = f"https://api.voluum.com/report/conversions?column=clickId&column=transactionId&column=visitTimestamp&column=postbackTimestamp&column=revenue&column=cost&column=profit&column=campaignId&column=campaignName&column=offerId&column=offerName&column=landerId&column=landerName&column=flowId&column=pathId&column=trafficSourceId&column=trafficSourceName&column=conversionType&column=conversionTypeId&column=referrer&column=affiliateNetworkId&column=affiliateNetworkName&column=countryCode&column=countryName&column=region&column=city&column=ip&column=isp&column=connectionType&column=deviceName&column=os&column=osVersion&column=browser&column=browserVersion&column=userAgent&column=language&column=status&column=customVariable1&column=customVariable2&column=customVariable3&column=customVariable4&column=customVariable5&column=customVariable6&column=customVariable7&column=customVariable8&column=customVariable9&column=customVariable10&column=externalId&column=externalIdType&from={from_date}&to={to_date}&limit={limit}&offset=OFFSET_VALUE&currency=EUR"
        access_token = self.create_session_token()

        headers = {
            "cwauth-token": access_token
        }
        total_data = []
        offset = 0

        while True:
            paginated_url = url.replace("OFFSET_VALUE", str(offset))
            response = requests.get(paginated_url, headers=headers)
            data = response.json()
            total_data.extend(data['rows'])
            print(f"Fetched {len(data['rows'])} rows at offset {offset} out of {data['totalRows']} total rows. Current total: {len(total_data)}")
            if len(total_data) == int(data["totalRows"]):
                break
            offset += int(limit)
        return total_data
    
    def decrypt_number(self, phone_number):
        if not phone_number or len(phone_number) < 25:
            return phone_number
        return encryption_handler.decrypt_number(phone_number)

    def get_offers_data(self, from_date="2025-10-01T00:00:00.000Z", to_date="2026-02-14T00:00:00.000Z"):
        limit = 1000
        url = f"https://panel-api2.voluum.com/report?reportType=tree&limit={limit}&dateRange=custom-date-time&from={from_date}&to={to_date}&searchMode=TEXT&include=ALL&currency=EUR&direction=DESC&offset=OFFSET_VALUE&groupBy=offer&conversionTimeMode=CONVERSION&column=offerId&column=offerName&tz=Etc/GMT"
        access_token = self.create_session_token()

        headers = {
            "cwauth-token": access_token
        }

        total_data = []
        offset = 0
        while True:
            paginated_url = url.replace("OFFSET_VALUE", str(offset))
            response = requests.get(paginated_url, headers=headers)
            data = response.json()
            total_data.extend(data['rows'])
            print(f"Fetched {len(data['rows'])} offers at offset {offset} out of {data['totalRows']} total. Current total: {len(total_data)}")
            if len(total_data) >= int(data["totalRows"]):
                break
            offset += limit
        return total_data

    def get_campaigns_data(self, from_date="2025-10-01T00:00:00.000Z", to_date="2026-02-14T00:00:00.000Z"):
        limit = 1000
        url = f"https://panel-api2.voluum.com/report?reportType=tree&limit={limit}&dateRange=custom-date-time&from={from_date}&to={to_date}&searchMode=TEXT&include=ALL&currency=EUR&direction=DESC&offset=OFFSET_VALUE&groupBy=campaign&conversionTimeMode=CONVERSION&column=campaignId&column=campaignName&tz=Etc/GMT"
        access_token = self.create_session_token()

        headers = {
            "cwauth-token": access_token
        }

        total_data = []
        offset = 0
        while True:
            paginated_url = url.replace("OFFSET_VALUE", str(offset))
            response = requests.get(paginated_url, headers=headers)
            data = response.json()
            total_data.extend(data['rows'])
            print(f"Fetched {len(data['rows'])} campaigns at offset {offset} out of {data['totalRows']} total. Current total: {len(total_data)}")
            if len(total_data) >= int(data["totalRows"]):
                break
            offset += limit
        return total_data

    def get_conversions_data_as_dataframe(self, limit="10000", from_date="2025-12-06T00:00:00.000Z", to_date="2025-12-13T00:00:00.000Z"):
        data = self.get_conversions_data(limit, from_date, to_date)
        df = pd.DataFrame(data)
        df["customVariable1"] = df["customVariable1"].apply(self.decrypt_number)
        return df
