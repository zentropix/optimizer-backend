import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone, UTC
import pandas as pd
import requests
import psycopg2
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from app.utils.voluum_access_key_handler import VoluumAccessKeyHandler
import time 
import threading

load_dotenv()
access_key_handler = VoluumAccessKeyHandler()
access_key_handler.get_access_token()

MAX_CONCURRENT_REQUESTS = 10   # <-- tune this (start low)
RATE_LIMIT_SLEEP = 0.25       # seconds between calls

semaphore = threading.Semaphore(MAX_CONCURRENT_REQUESTS)

class MMDDataHandler:
    def __init__(self):
        self.access_token = None
        self.links_dict = {}

    def get_broadcasts(self, date):
        # date = "2026-01-20"
        url = "https://sms.messagewhiz.com/api/3/Broadcast/List"

        start = 0  # Initialize starting point for pagination
        total_fetched = 0 
        list_of_broadcasts = []
        dt = datetime.strptime(date, "%Y-%m-%d")

        # subtract one day
        prev_day = dt - timedelta(days=1)

        # format back to same string format
        previous_date = prev_day.strftime("%Y-%m-%d")
        global_check = True
        while True:
            params = {
                'limit': 100,
                'start': start,
            }

            headers = {
                "apikey": "fc416a66-fdcc-480b-a693-f85007723364",
                "Content-Type": "application/json"
            }

            # Send the request
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Failed to retrieve data: {response.status_code}")
                break   
            # year, month, day = date.split("-")
            broadcast_json = response.json()
            broadcast_res = broadcast_json["result"]
            count = len(broadcast_res)  # Count the number of items fetched in this iteration
            # Process each item
            for item in broadcast_res:
                if date in item["send_date"]:
                    list_of_broadcasts.append(item)
                else:
                    if previous_date in item["send_date"]:
                        global_check = False           
                        break
            total_fetched += count

            if not global_check:
                break

            if count < 100:  # If less than 100 items, stop fetching
                break

            start += count
        
        return list_of_broadcasts
    
    def get_broadcasts_until_day(self, date):
        url = "https://sms.messagewhiz.com/api/3/Broadcast/List"

        start = 0  # Initialize starting point for pagination
        total_fetched = 0 
        list_of_broadcasts = []
        dt = datetime.strptime(date, "%Y-%m-%d")

        # subtract one day
        prev_day = dt - timedelta(days=1)

        # format back to same string format
        previous_date = prev_day.strftime("%Y-%m-%d")
        global_check = True
        while True:
            params = {
                'limit': 100,
                'start': start,
            }

            headers = {
                "apikey": "fc416a66-fdcc-480b-a693-f85007723364",
                "Content-Type": "application/json"
            }

            # Send the request
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Failed to retrieve data: {response.status_code}")
                break   
            # year, month, day = date.split("-")
            broadcast_json = response.json()
            broadcast_res = broadcast_json["result"]
            count = len(broadcast_res)  # Count the number of items fetched in this iteration
            # Process each item
            for item in broadcast_res:
                list_of_broadcasts.append(item)
                if previous_date in item["send_date"]:
                    global_check = False           
                    break
            total_fetched += count

            if not global_check:
                break

            if count < 100:  # If less than 100 items, stop fetching
                break

            start += count
        
        return list_of_broadcasts

    def get_links(self):
        url = "https://sms.messagewhiz.com/api/3/Link"

        start = 0  # Initialize starting point for pagination
        total_fetched = 0 
        list_of_links = []
        
        global_check = True
        while True:
            params = {
                'limit': 100,
                'start': start,
            }

            headers = {
                "apikey": os.getenv("MMD_API_KEY"),
                "Content-Type": "application/json"
            }

            # Send the request
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Failed to retrieve data: {response.status_code}")
                break   

            link_json = response.json()
            link_res = link_json["result"]
            count = len(link_res)  # Count the number of items fetched in this iteration
            # Process each item
            for item in link_res:
                list_of_links.append(item)

            total_fetched += count

            if not global_check:
                break

            if count < 100:  # If less than 100 items, stop fetching
                break

            start += count
        
        for link in list_of_links:
            self.links_dict[link["id"]] = link["url"].split("?")[0]

    def extract_link_id(self, message):
        return message.split("{{link:")[1].split("}}")[0]

    def get_url(self, link_id):
        return self.links_dict.get(link_id, None)

    def get_campaign_id(self, link):
        return link.split("m/")[1] if link else None

    def format_real_price(self, price):
        return price / 100

    def rate_limited_get_campaign(self, *args, **kwargs):
        with semaphore:
            time.sleep(RATE_LIMIT_SLEEP)  # smooths bursts
            return self.get_campaign_data(*args, **kwargs)
    
    def get_campaign_data(self, campaign_id, from_date, to_date):
        access_token = access_key_handler.get_access_token()
        url = f"https://panel-api2.voluum.com/report?reportType=table&limit=1&dateRange=yesterday&from={from_date}&to={to_date}&searchMode=TEXT&currency=EUR&sort=visits&direction=ASC&reportDataType=0&offset=0&groupBy=custom-variable-1&groupBy=ip&groupBy=browser-version&groupBy=os-version&column=profit&column=customVariable1&column=visits&column=uniqueVisits&column=suspiciousVisitsPercentage&column=campaignNoConversionsWarning&column=conversions&column=costSources&column=cost&column=revenue&column=roi&column=cv&column=epv&column=cpv&column=errors&column=Click2Reg&column=Reg2FTD&column=customConversions1&column=customRevenue1&column=customConversions2&column=customRevenue2&column=customConversions3&column=customRevenue3&column=osVersion&column=browserVersion&column=actions&column=type&column=clicks&column=suspiciousClicksPercentage&column=suspiciousVisits&column=suspiciousClicks&tz=Etc/GMT&filter1=campaign&filter1Value={campaign_id}&tz=Utc"
        headers = {
            "cwauth-token": access_token
        }
        try:
            response = requests.get(url, headers=headers)
            campaigns = response.json()
        except Exception as e:
            print(f"Error fetching campaign data for campaign ID {campaign_id}: {e}")
            time.sleep(10)
            response = requests.get(url, headers=headers)
            campaigns = response.json()
        return campaigns

    def add_broadcast_campaign_data(self, broadcast_df, search_filter=""):
        grouped_rows = {}
        selected_rows = []

        # ----------------------------
        # STEP 1: FILTER & LIMIT FIRST
        # ----------------------------
        for _, row in broadcast_df.iterrows():
            if row["state"] in [0, 2]:
                continue

            if search_filter.lower() not in row["name"].lower():
                continue

            broadcast_name = row["name"].split("_chunk")[0]

            if broadcast_name not in grouped_rows:
                grouped_rows[broadcast_name] = []

            if len(grouped_rows[broadcast_name]) >= 10:
                continue

            grouped_rows[broadcast_name].append(None)  # placeholder
            selected_rows.append((broadcast_name, row))

        # Reset grouped_rows for actual data
        grouped_rows = {k: [] for k in grouped_rows.keys()}

        # ----------------------------
        # STEP 2: API WORKER
        # ----------------------------
        def process_row(broadcast_name, row):
            date, time = row["send_date"].split("T")
            hour = int(time.split(":")[0])

            from_date = f"{date}T{str(hour).zfill(2)}:00:00.000Z"

            later_hour = (hour + 1) % 24
            year, month, day = date.split("-")
            to_date_day = int(day) + 1 if later_hour == 0 else int(day)

            to_date = (
                f"{year}-{month}-{str(to_date_day).zfill(2)}"
                f"T{str(later_hour).zfill(2)}:00:00.000Z"
            )

            totals = self.rate_limited_get_campaign(
                row["campaign_id"],
                from_date=from_date,
                to_date=to_date,
            )
            try:
                totals = totals["totals"]
                return broadcast_name, {
                    "Broadcast ID": row["id"],
                    "Name": row["name"],
                    "Send Date": row["send_date"],

                    "Real Price": round(row["real_price"], 2),
                    "Estimated Price": round(row["estimated_price"] / 100, 2),
                    "Recipient Count": row["recipient_count"],

                    "DLR Sent Count": row["dlr"]["sent_count"] if row.get("dlr") else 0,
                    "DLR Delivered Count": row["dlr"]["delivered_count"] if row.get("dlr") else 0,
                    "DLR Undelivered Count": row["dlr"]["undelivered_count"] if row.get("dlr") else 0,
                    "DLR Rejected Count": row["dlr"]["rejected_count"] if row.get("dlr") else 0,
                    "DLR Expired Count": row["dlr"]["expired_count"] if row.get("dlr") else 0,
                    "DLR Failed Count": row["dlr"]["failed_count"] if row.get("dlr") else 0,
                    "DLR Read Count": row["dlr"]["read_count"] if row.get("dlr") else 0,

                    "BC Unique Clicks": row["broadcastConversion"]["uniqueClicks"],
                    "BC Total Clicks": row["broadcastConversion"]["totalClicks"],
                    "BC Recipient Count": row["broadcastConversion"]["recipientCount"],
                    "CTR": f'{round(float(row["broadcastConversion"]["conversion"]), 2)}%',

                    "unique_visits": totals["uniqueVisits"],
                    "visits": totals["visits"],
                    "Click 2 REG": f'{round(totals["Click2Reg"], 2)}%',
                    "Reg 2 FTD": f'{totals["Reg2FTD"]}%',
                    "Clicks": totals["clicks"],
                    "Conversions": totals["conversions"],
                    "revenue": totals["revenue"],

                    "message_body": row["message_body"],
                }

            except Exception as e:
                print(f"❌ Error campaign {row['campaign_id']}: {e}")
                return None

        # ----------------------------
        # STEP 3: EXECUTE ONLY REQUIRED CALLS
        # ----------------------------
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(process_row, broadcast_name, row)
                for broadcast_name, row in selected_rows
            ]

            for future in tqdm(as_completed(futures), total=len(futures)):
                result = future.result()
                if not result:
                    continue

                broadcast_name, data = result
                grouped_rows[broadcast_name].append(data)

        return grouped_rows
    
    def get_live_data(self, date, search_filter=""):
        broadcasts = self.get_broadcasts(date)
        print(f"Number of broadcasts fetched for date: {date} are {len(broadcasts)}")

        # Get links
        self.get_links()

        # Process broadcasts into DataFrame
        broadcast_df = pd.DataFrame(broadcasts)
        broadcast_df["link_id"] = broadcast_df["message_body"].apply(lambda x: x.split("{{link:")[1].split("}}")[0])
        broadcast_df["url"] = broadcast_df["link_id"].apply(lambda x: self.links_dict.get(int(x), None))
        broadcast_df["campaign_id"] = broadcast_df["url"].apply(lambda x: x.split("m/")[1] if x else None)
        broadcast_df["real_price"] = broadcast_df["real_price"].apply(lambda price: price / 100)

        grouped_rows = self.add_broadcast_campaign_data(broadcast_df, search_filter)
        return grouped_rows
    
    def process_single_row(self, row, grouped_rows):
        date, time = row["send_date"].split("T")
        hour = int(time.split(":")[0])

        from_dt = datetime.strptime(
            f"{date} {hour:02d}:00:00",
            "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)

        # Current time rounded down to hour
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

        # Decide to_date
        if now - from_dt > timedelta(hours=24):
            to_dt = from_dt + timedelta(hours=24)
        else:
            to_dt = now

        # Format back to string
        from_date = from_dt.strftime("%Y-%m-%dT%H:00:00.000Z")
        to_date = to_dt.strftime("%Y-%m-%dT%H:00:00.000Z")
        if (from_date, to_date, row["campaign_id"]) not in grouped_rows:
            grouped_rows[from_date, to_date, row["campaign_id"]] = self.rate_limited_get_campaign(
                                                                    row["campaign_id"],
                                                                    from_date=from_date,
                                                                    to_date=to_date
                                                                )["totals"]
        totals = grouped_rows[from_date, to_date, row["campaign_id"]]

        return grouped_rows, {
            "Campaign ID": row["campaign_id"],
            "Broadcast ID": row["id"],
            "Name": row["name"],
            "Send Date": row["send_date"].split(".")[0],

            # "Real Price": round(row["real_price"], 2),
            "Estimated Price": round(row["estimated_price"] / 100, 2),
            "Recipient Count": row["recipient_count"],

            "BC Unique Clicks": row["broadcastConversion"]["uniqueClicks"],
            "BC Total Clicks": row["broadcastConversion"]["totalClicks"],
            # "BC Recipient Count": row["broadcastConversion"]["recipientCount"],
            "CTR": f'{round(float(row["broadcastConversion"]["conversion"]), 2)}%',

            "DLR Delivered Count": row["dlr"]["delivered_count"] if row.get("dlr") else 0,
            "DLR Sent Count": row["dlr"]["sent_count"] if row.get("dlr") else 0,
            "DLR Undelivered Count": row["dlr"]["undelivered_count"] if row.get("dlr") else 0,
            "DLR Rejected Count": row["dlr"]["rejected_count"] if row.get("dlr") else 0,
            # "DLR Expired Count": row["dlr"]["expired_count"] if row.get("dlr") else 0,
            # "DLR Failed Count": row["dlr"]["failed_count"] if row.get("dlr") else 0,
            # "DLR Read Count": row["dlr"]["read_count"] if row.get("dlr") else 0,


            "Voluum Visits": totals["visits"],
            "Voluum Unique Visits": totals["uniqueVisits"],
            "Click 2 REG": f'{round(totals["Click2Reg"], 2)}%',
            "Reg 2 FTD": f'{totals["Reg2FTD"]}%',
            # "Clicks": totals["clicks"],
            # "Conversions": totals["conversions"],
            # "revenue": totals["revenue"],

            "message_body": row["message_body"],
        }
    
    def update_prices(self):
        date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        # date = "2026-01-27"
        # next_date = "2026-01-28"
        next_date = datetime.utcnow().strftime("%Y-%m-%d")
        print(f'Updating costs for date {date} to date: {next_date}')
        broadcasts = self.get_broadcasts(date)
        
        self.get_links()

        broadcast_df = pd.DataFrame(broadcasts)
        broadcast_df["link_id"] = broadcast_df["message_body"].apply(lambda x: x.split("{{link:")[1].split("}}")[0])
        broadcast_df["url"] = broadcast_df["link_id"].apply(lambda x: self.links_dict.get(int(x), None))
        broadcast_df["campaign_id"] = broadcast_df["url"].apply(lambda x: x.split("m/")[1] if x else None)
        broadcast_df["real_price"] = broadcast_df["real_price"].apply(lambda price: price / 100)
        cost_update_url = "https://api.voluum.com/report/cost"
        headers = {
            "cwauth-token": access_key_handler.get_access_token()
        }

        campaign_ids = broadcast_df["campaign_id"].unique().tolist()
        i=0
        cost_updates = []
        for id in campaign_ids:
            rows = broadcast_df[broadcast_df.campaign_id==id]
            total_cost = rows['real_price'].sum()
            body = {
                "costUpdateRequests":[
                    {
                        "campaignId": rows.iloc[0]['campaign_id'],
                        "from":f"{date}T00:00:00Z",
                        "to":f"{next_date}T00:00:00Z",
                        "timezone":"Etc/GMT",
                        "cost":total_cost,
                        "currency":"USD"
                    }
                ]
            }
            response = requests.post(cost_update_url, headers=headers, json=body)
            cost_updates.append({
                    "index": str(i), 
                    "rows": len(rows), 
                    "campaign_id": rows.iloc[0]['campaign_id'], 
                    "total_cost": round(total_cost, 3), 
                    "chunk": rows.iloc[0]["name"], 
                    "response": response.status_code
                })
            i+=1
            time.sleep(5)
        return {
            "total_rows": len(campaign_ids),
            "updates": cost_updates
        }

    def save_data_for_day(self, previous_date):
        broadcasts = self.get_broadcasts(previous_date)
        self.get_links()
        broadcast_df = pd.DataFrame(broadcasts)
        broadcast_df["link_id"] = broadcast_df["message_body"].apply(lambda x: x.split("{{link:")[1].split("}}")[0])
        broadcast_df["url"] = broadcast_df["link_id"].apply(lambda x: self.links_dict.get(int(x), None))
        broadcast_df["campaign_id"] = broadcast_df["url"].apply(lambda x: x.split("m/")[1] if x else None)
        broadcast_df["real_price"] = broadcast_df["real_price"].apply(lambda price: price / 100)

        total_rows = []
        grouped_rows = {}
        for _, row in tqdm(broadcast_df.iterrows(), total=len(broadcast_df)):
            if row["state"] in [0, 2]:
                continue
            grouped_rows, processed_row = self.process_single_row(row, grouped_rows)
            total_rows.append(processed_row)
        total_rows_df = pd.DataFrame(total_rows)

        return total_rows_df, len(total_rows_df)

if __name__ == "__main__":
    # date = "2026-01-19"
    mmd_handler = MMDDataHandler()
    # rows_to_insert = mmd_handler.get_live_data(date)
    # grouped_rows = {}
    # for _, row in rows_to_insert.iterrows():
    #     broadcast_name = row["name"].split("_chunk")[0]
    #     if broadcast_name not in grouped_rows:
    #         grouped_rows[broadcast_name] = []
    #     grouped_rows[broadcast_name].append(row)
    # print(grouped_rows)
    print(mmd_handler.save_data_for_day("2026-01-28"))
