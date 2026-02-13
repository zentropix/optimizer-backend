import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from datetime import datetime
from time import time
import pandas as pd
import numpy as np
import json
from contextlib import contextmanager
from datetime import datetime
from app.utils.encryption_handler import EncryptionHandler
from pathlib import Path
import csv

load_dotenv()

class DBHandler:
    def __init__(self, batch_size=10000):
        self.db_host = os.getenv("DB_HOST")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_pass = os.getenv("DB_PASSWORD")
        self.connect()
        self.batch_size = batch_size
        # Loading the country code file
        self.country_codes = json.load(open('app/utils/country_codes.json', 'r'))
        self.calling_codes = json.load(open('app/utils/calling_codes.json', 'r'))
        # Build a sorted list of (prefix, iso2) longest-first for phone number matching
        self._calling_code_prefixes = sorted(
            self.calling_codes.items(),
            key=lambda x: len(x[0]),
            reverse=True,
        )
        self.encryption_handler = EncryptionHandler()

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_user,
                password=self.db_pass
            )
        except Exception as e:
            self.connection = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_user,
                password=self.db_pass
            )

    def get_country_code_from_phone(self, phone: str) -> str | None:
        """
        Detect ISO2 country code from a phone number by matching the calling code prefix.
        Tries longest prefix first (e.g. '1242' for Bahamas before '1' for US).
        Returns the ISO2 code (e.g. 'BE', 'US') or None if no match.
        """
        if not phone:
            return None
        # Strip any leading '+' or '00'
        phone = str(phone).strip()
        if phone.startswith('+'):
            phone = phone[1:]
        elif phone.startswith('00'):
            phone = phone[2:]

        for prefix, iso2 in self._calling_code_prefixes:
            if phone.startswith(prefix):
                return iso2
        return None

    @contextmanager
    def _cursor(self):
        """
        Safe cursor context manager.
        - Reconnects if the connection is closed/broken.
        - On success: the caller is responsible for commit().
        - On exception: automatically rolls back so the connection
          stays usable for other queries.
        """
        if self.connection.closed:
            self.connect()
        try:
            with self.connection.cursor() as cursor:
                yield cursor
        except Exception:
            self.connection.rollback()
            raise

    def get_all_records_from_table(self, table_name):
        query = f"SELECT * FROM \"{table_name}\";"
        with self._cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()
        return records

    def insert_raw_data_into_ts_source(self, data):
        insert_query = f"""
        INSERT INTO public."api_voluum_ts_sources" ( click_2_reg, reg_2_ftd, browser_version, custom_variable_1, timestamp_created, category, cost, conversions, clicks, cost_sources, cpv, custom_conversions_1, custom_conversions_2, custom_conversions_3, custom_revenue_1, custom_revenue_2, custom_revenue_3, cv, epv, errors, profit, revenue, roi, suspicious_clicks, suspicious_clicks_percentage, suspicious_visits, suspicious_visits_percentage, unique_visits, visits, ip, os_version, data_source
        ) VALUES %s
        """
        # 14, 18, 19, 2, timestamp, 29, 0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 20, 21, 22, 23, 24, 25, 26, 27, 28
        rows = []
        now = datetime.now()
        start_time = time()
        for record in data:
            rows.append((
                record[0], record[1], record[2], int(record[14]), now, record[29], record[5], record[4], record[3], ",".join(record[6]), record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[15], record[16], record[17], record[20], record[21], record[22], record[23], record[24], record[25], record[26], record[27], record[28], record[18], record[19], "VOLUUM"
            ))
        print(f"Data prepared for insertion in {time() - start_time} seconds.")
        if rows:
            with self._cursor() as cursor:
                execute_values(cursor, insert_query, rows, page_size=self.batch_size)
            self.connection.commit()
            print(f"Inserted final batch of {len(rows)} records into api_voluum_ts_sources.")
        return True

    def upsert_data_into_whitelist(self, table_name, data):
        """
        UPSERT data into whitelist table.
        - Inserts new records
        - Updates existing records based on custom_variable_1
        - Detects country_code from the phone number (custom_variable_1)
        """

        query = f"""
        INSERT INTO public."whitelist" ( custom_variable_1, ip, os_version, browser_version, timestamp_created, source, click_2_reg, reg_2_ftd, clicks, conversions, cost, cost_sources, cpv, custom_conversions_1, custom_conversions_2, custom_conversions_3, custom_revenue_1, custom_revenue_2, custom_revenue_3, cv, epv, errors, profit, revenue, roi, suspicious_clicks, suspicious_clicks_percentage, suspicious_visits, suspicious_visits_percentage, unique_visits, visits, country_code
        )
        VALUES %s
        ON CONFLICT (custom_variable_1)
        DO UPDATE SET
            ip = EXCLUDED.ip,
            os_version = EXCLUDED.os_version,
            browser_version = EXCLUDED.browser_version,
            click_2_reg = EXCLUDED.click_2_reg,
            reg_2_ftd = EXCLUDED.reg_2_ftd,
            clicks = EXCLUDED.clicks,
            conversions = EXCLUDED.conversions,
            cost = EXCLUDED.cost,
            cost_sources = EXCLUDED.cost_sources,
            cpv = EXCLUDED.cpv,
            custom_conversions_1 = EXCLUDED.custom_conversions_1,
            custom_conversions_2 = EXCLUDED.custom_conversions_2,
            custom_conversions_3 = EXCLUDED.custom_conversions_3,
            custom_revenue_1 = EXCLUDED.custom_revenue_1,
            custom_revenue_2 = EXCLUDED.custom_revenue_2,
            custom_revenue_3 = EXCLUDED.custom_revenue_3,
            cv = EXCLUDED.cv,
            epv = EXCLUDED.epv,
            errors = EXCLUDED.errors,
            profit = EXCLUDED.profit,
            revenue = EXCLUDED.revenue,
            roi = EXCLUDED.roi,
            suspicious_clicks = EXCLUDED.suspicious_clicks,
            suspicious_clicks_percentage = EXCLUDED.suspicious_clicks_percentage,
            suspicious_visits = EXCLUDED.suspicious_visits,
            suspicious_visits_percentage = EXCLUDED.suspicious_visits_percentage,
            unique_visits = EXCLUDED.unique_visits,
            visits = EXCLUDED.visits,
            country_code = EXCLUDED.country_code
        ;
        """
        now = datetime.now()
        rows = []

        for record in data:
            phone = str(record[14])
            country_code = self.get_country_code_from_phone(phone)
            rows.append((
                int(record[14]), record[18], record[19], record[2], now, "VOLUUM", record[0], record[1], record[3], record[4], record[5], ",".join(record[6]), record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[15], record[16], record[17], record[20], record[21], record[22], record[23], record[24], record[25], record[26], record[27], record[28], country_code
        ))

        # Flush remaining
        if rows:
            with self._cursor() as cursor:
                execute_values(cursor, query, rows, page_size=self.batch_size)
            keys = [row[0] for row in rows]
            self.remove_from_other_tables(keys, ["blacklist", "monitor"])
            self.connection.commit()
            print(f"Inserted/Updated final batch of {len(rows)} records into {table_name}.")

        return len(rows)

    def upsert_data_into_main_database(self, data):
        """
        UPSERT data into main_database table.
        - Inserts new records
        - Updates existing records based on custom_variable_1
        """

        query = f"""
        INSERT INTO public."main_database" ( custom_variable_1)
        VALUES %s
        ON CONFLICT (custom_variable_1)
        DO NOTHING;
        """
        rows = []

        for record in data:
            rows.append((int(record),))
        # Flush remaining
        if rows:
            with self._cursor() as cursor:
                execute_values(cursor, query, rows, page_size=self.batch_size)
            self.connection.commit()
            print(f"Inserted/Updated final batch of {len(rows)} records into Main Database.")

        return len(rows)

    def upsert_data_into_blacklist_and_monitor(self, table_name, data):
        """
        UPSERT data into blacklist and monitor tables.
        - Inserts new records
        - Updates existing records based on custom_variable_1
        """

        query = f"""
        INSERT INTO public."{table_name}" ( custom_variable_1, ip, os_version, browser_version, timestamp_created, reason, source, click_2_reg, reg_2_ftd, clicks, conversions, cost, cost_sources, cpv, custom_conversions_1, custom_conversions_2, custom_conversions_3, custom_revenue_1, custom_revenue_2, custom_revenue_3, cv, epv, errors, profit, revenue, roi, suspicious_clicks, suspicious_clicks_percentage, suspicious_visits, suspicious_visits_percentage, unique_visits, visits
        )
        VALUES %s
        ON CONFLICT (custom_variable_1)
        DO UPDATE SET
            ip = EXCLUDED.ip,
            os_version = EXCLUDED.os_version,
            browser_version = EXCLUDED.browser_version,
            reason = EXCLUDED.reason,
            click_2_reg = EXCLUDED.click_2_reg,
            reg_2_ftd = EXCLUDED.reg_2_ftd,
            clicks = EXCLUDED.clicks,
            conversions = EXCLUDED.conversions,
            cost = EXCLUDED.cost,
            cost_sources = EXCLUDED.cost_sources,
            cpv = EXCLUDED.cpv,
            custom_conversions_1 = EXCLUDED.custom_conversions_1,
            custom_conversions_2 = EXCLUDED.custom_conversions_2,
            custom_conversions_3 = EXCLUDED.custom_conversions_3,
            custom_revenue_1 = EXCLUDED.custom_revenue_1,
            custom_revenue_2 = EXCLUDED.custom_revenue_2,
            custom_revenue_3 = EXCLUDED.custom_revenue_3,
            cv = EXCLUDED.cv,
            epv = EXCLUDED.epv,
            errors = EXCLUDED.errors,
            profit = EXCLUDED.profit,
            revenue = EXCLUDED.revenue,
            roi = EXCLUDED.roi,
            suspicious_clicks = EXCLUDED.suspicious_clicks,
            suspicious_clicks_percentage = EXCLUDED.suspicious_clicks_percentage,
            suspicious_visits = EXCLUDED.suspicious_visits,
            suspicious_visits_percentage = EXCLUDED.suspicious_visits_percentage,
            unique_visits = EXCLUDED.unique_visits,
            visits = EXCLUDED.visits
        ;
        """

        now = datetime.now()
        rows = []

        for record in data:
            rows.append((
                int(record[14]), record[18], record[19], record[2], now, "OS_VERSION", "VOLUUM", record[0], record[1], record[3], record[4], record[5], ",".join(record[6]), record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[15], record[16], record[17], record[20], record[21], record[22], record[23], record[24], record[25], record[26], record[27], record[28]
            ))

        # Flush remaining
        if rows:
            with self._cursor() as cursor:
                execute_values(cursor, query, rows, page_size=self.batch_size)
            self.remove_from_other_tables([row[0] for row in rows], ["whitelist", "blacklist" if table_name == "monitor" else "monitor"])
            self.connection.commit()
            print(f"Inserted/Updated final batch of {len(rows)} records into {table_name}.")
        return len(rows)

    def empty_all_tables(self):
        tables = ["api_voluum_ts_sources", "blacklist", "whitelist", "monitor"]
        for table in tables:
            delete_query = f'DELETE FROM "{table}";'
            with self._cursor() as cursor:
                cursor.execute(delete_query)
            self.connection.commit()
        return True

    def clean_csv_records(self, data, include_main_database, include_conversion, include_blacklist, include_monitor):
        values = [row['record'] for row in data]
        cleaned_values = values
        removed_blacklist_count, removed_monitor_count, removed_conversion_count, removed_main_database_count = 0, 0, 0, 0
        if include_blacklist:
            cleaned_values = self.filter_existing_keys(values, 'blacklist')
            removed_blacklist_count = len(values) - len(cleaned_values)
        if include_monitor:
            cleaned_values = self.filter_existing_keys(cleaned_values, 'monitor')
            removed_monitor_count = len(values) - removed_blacklist_count - len(cleaned_values)
        if include_conversion:
            cleaned_values = self.filter_existing_keys(cleaned_values, 'api_voluum_conversions')
            removed_conversion_count = len(values) - removed_blacklist_count - removed_monitor_count - len(cleaned_values)
        if include_main_database:
            cleaned_values = self.filter_existing_keys(cleaned_values, "main_database")
            removed_main_database_count = len(values) - removed_blacklist_count - removed_monitor_count - removed_conversion_count - len(cleaned_values)
            self.upsert_data_into_main_database(cleaned_values)
        return cleaned_values, removed_blacklist_count, removed_monitor_count, removed_conversion_count, removed_main_database_count

    def encrypt_and_save_csv(self, phone_numbers):
        return self.encryption_handler.encrypt_list(phone_numbers)

    def filter_existing_keys(self, keys: list[str], table_name: str) -> list[str]:
        """
        Remove keys that already exist in the specified table.

        Args:
            keys: list of custom_variable_1 values
            table_name: table to check for existing keys
            batch_size: process in chunks to avoid huge queries

        Returns:
            list of keys NOT in the table
        """
        if not keys:
            return []

        remaining_keys = set(keys)  # start with all keys

        # Process in batches
        for i in range(0, len(keys), self.batch_size):
            chunk = keys[i:i+self.batch_size]
            query = f'SELECT custom_variable_1 FROM public."{table_name}" WHERE custom_variable_1 = ANY(%s)'
            with self._cursor() as cursor:
                cursor.execute(query, (chunk,))
                existing = {row[0] for row in cursor.fetchall()}

            # Remove existing keys from remaining_keys
            remaining_keys -= existing

        return list(remaining_keys)

    def remove_from_other_tables(self, keys, tables):
        """
        Delete rows from other tables if the key exists.
        keys: list of custom_variable_1
        tables: list of table names to delete from
        """
        if not keys:
            return

        for table in tables:
            query = f'DELETE FROM "{table}" WHERE custom_variable_1 = ANY(%s::varchar[])'
            with self._cursor() as cursor:
                cursor.execute(query, (keys,))
            self.connection.commit()
            print(f"Deleted rows from {table}.")

    def find_records_in_ts_source(self, key):
        """
        Check which keys exist in the specified table.
        keys: list of custom_variable_1
        Returns list of existing keys.
        """

        query = f'SELECT * FROM public."api_voluum_ts_sources" WHERE custom_variable_1 = \'{key}\''
        with self._cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()
        return records

    def insert_conversions(self, data):
        insert_query = f"""
        INSERT INTO public."api_voluum_conversions" (click_id, postback_timestamp, processed, processed_at, error_message, retry_count, last_retry_at, generated_email, custom_variable_1, affiliate_network_id, affiliate_network_name, browser, browser_version, campaign_id, campaign_name, city, connection_type, conversion_type, conversion_type_id, cost, country_code, country_name, custom_variable_10, custom_variable_2, custom_variable_3, custom_variable_4, custom_variable_5, custom_variable_6, custom_variable_7, custom_variable_8, custom_variable_9, device, device_name, external_id, external_id_type, flow_id, ip, isp, lander_id, lander_name, language, offer_id, offer_name, os, os_version, path_id, profit, referrer, region, revenue, traffic_source_id, traffic_source_name, transaction_id, user_agent, visit_timestamp, source)
        VALUES %s
        ON CONFLICT (click_id, transaction_id, conversion_type)
        DO NOTHING;
        """
        # 7, 39, false, now, null, 0, null, generated_email, 14, 0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 40, 41, 42, 43, 44, 45, 46, 47, 48
        rows = []
        now = datetime.now()
        start_time = time()
        for record in data:
            country_code = self.country_codes.get(record[5].split(' - ')[1], "DEF")
            conversion_type = "REG" if record[9] != "FTD" else "FTD"
            generated_email = '+' + record[14] + '@yourmobile.com' if record[14] and record[14].strip() and record[14].strip().lower() != '<na>' else None
            rows.append((
                record[7], record[39], False, now, None, 0, None, generated_email,
                record[14], record[0], record[1], record[2], record[3], record[4],
                record[5], record[6], record[8], conversion_type, record[10], record[11], country_code,
                record[13], record[15], record[16], record[17], record[18], record[19],
                record[20], record[21], record[22], record[23], record[24], record[25],
                record[26], record[27], record[28], record[29], record[30], record[31],
                record[32], record[33], record[34], record[35], record[36], record[37],
                record[38], record[40], record[41], record[42], record[43], record[44],
                record[45], record[46], record[47], record[48], country_code + "_" + conversion_type
            ))

        print(f"Data prepared for insertion in {time() - start_time} seconds.")
        if rows:
            with self._cursor() as cursor:
                execute_values(cursor, insert_query, rows, page_size=self.batch_size)
            self.connection.commit()
            print(f"Inserted final batch of {len(rows)} records into api_voluum_conversions.")
        return True

    def update_successful_conversions(self, emails):
        """
        emails: list[dict] where each item contains unique identifiers:
        - click_id (or clickId)
        - postback_timestamp (or postbackTimestamp)  (datetime | iso str | epoch seconds/ms)

        Updates matching rows by (click_id, postback_timestamp):
        processed = TRUE
        processed_at = <now or provided>
        error_message = NULL
        retry_count = 0
        last_retry_at = NULL
        """
        if not emails:
            return True

        now = datetime.now()

        # Build VALUES rows: (click_id, postback_timestamp, processed_at)
        rows = []
        seen = set()
        for item in emails:
            if not isinstance(item, dict):
                continue
            click_id = item.get("fields").get("click_id")
            postback_ts = item.get("fields").get("conversion_date")
            if click_id is None or postback_ts is None:
                continue

            click_id = str(click_id).strip()

            # allow per-item processed_at override, else now
            processed_at = now

            key = (click_id, postback_ts)
            if key in seen:
                continue
            seen.add(key)

            rows.append((click_id, postback_ts, processed_at))

        if not rows:
            return True
        update_query = """
        UPDATE public."api_voluum_conversions" AS c
        SET
            processed = TRUE,
            processed_at = v.processed_at,
            error_message = NULL,
            retry_count = 0,
            last_retry_at = NULL
        FROM (VALUES %s) AS v(click_id, postback_timestamp, processed_at)
        WHERE c.click_id = v.click_id
         AND c.postback_timestamp = to_timestamp(
            v.postback_timestamp,
            'YYYY-MM-DD HH12:MI:SS AM'
          )
        AND c.processed = FALSE
        """

        with self._cursor() as cursor:
            execute_values(cursor, update_query, rows, page_size=self.batch_size)

        self.connection.commit()
        return True

    def update_unsuccessful_conversions(self, emails, reason='INAVLID_PHONE_NUMBER'):
        """
        emails: list[dict] where each item contains unique identifiers:
        - click_id (or clickId)
        - postback_timestamp (or postbackTimestamp)  (datetime | iso str | epoch seconds/ms)

        Updates matching rows by (click_id, postback_timestamp):
        processed = TRUE
        processed_at = <now or provided>
        error_message = NULL
        retry_count = 0
        last_retry_at = NULL
        """
        if not emails or not len(emails):
            return True

        now = datetime.now()

        # Build VALUES rows: (click_id, postback_timestamp, processed_at)
        rows = []
        seen = set()
        for item in emails:
            if not isinstance(item, dict):
                continue
            click_id = item.get("click_id")
            postback_ts = item.get("conversion_date")
            if click_id is None or postback_ts is None:
                continue

            click_id = str(click_id).strip()

            # allow per-item processed_at override, else now
            processed_at = now

            key = (click_id, postback_ts)
            if key in seen:
                continue
            seen.add(key)

            rows.append((click_id, postback_ts, processed_at))

        if not rows:
            return True
        update_query = f"""
        UPDATE public."api_voluum_conversions" AS c
        SET
            processed = FALSE,
            processed_at = v.processed_at,
            error_message = '{reason}',
            retry_count = c.retry_count + 1,
            last_retry_at = v.processed_at
        FROM (VALUES %s) AS v(click_id, postback_timestamp, processed_at)
        WHERE c.click_id = v.click_id
         AND c.postback_timestamp = to_timestamp(
            v.postback_timestamp,
            'YYYY-MM-DD HH12:MI:SS AM'
          )
        AND c.processed = FALSE
        """

        with self._cursor() as cursor:
            execute_values(cursor, update_query, rows, page_size=self.batch_size)

        self.connection.commit()
        return True

    def get_counts(self, table_name: str, column_name: str):
        """
        Returns (total, last_24_hours, last_7_days)
        """
        q = sql.SQL("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE {col} >= NOW() - INTERVAL '24 hours') AS last_24_hours,
                COUNT(*) FILTER (WHERE {col} >= NOW() - INTERVAL '7 days')  AS last_7_days
            FROM {tbl};
        """).format(
            tbl=sql.Identifier(table_name),
            col=sql.Identifier(column_name),
        )

        with self._cursor() as cursor:
            cursor.execute(q)
            total, last_24h, last_7d = cursor.fetchone()
        return total, last_24h, last_7d

    def create_file_entry(self, file_id, original_filename: str, raw_file_path: str, record_count_total: str) -> str:
        """
        Insert a new file entry and return its UUID (as str).
        """
        q = """
            INSERT INTO uploaded_csv_files (id, original_filename, raw_file_path, status, record_count_total)
            VALUES (%s::uuid, %s, %s, 'uploaded', %s)
            RETURNING id::text AS id;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id, original_filename, raw_file_path, record_count_total))
            created = cursor.fetchone()
        self.connection.commit()
        return {"id": created[0], "original_filename": original_filename, "status": "uploaded"}

    def list_files(self, statuses=None):
        if statuses:
            q = """
                SELECT id::text AS id, original_filename, status,
                    record_count_total, record_count_clean,
                    uploaded_at, processing_started_at, processed_at, error_message,
                    hlr_status, hlr_batch_id, hlr_started_at, hlr_completed_at,
                    hlr_num_items, hlr_num_complete, hlr_error_message, hlr_raw_file_path
                FROM uploaded_csv_files
                WHERE status = ANY(%s)
                ORDER BY uploaded_at DESC;
            """
            with self._cursor() as cursor:
                cursor.execute(q, (statuses,))
                rows = cursor.fetchall()
        else:
            q = """
                SELECT id::text AS id, original_filename, status,
                    record_count_total, record_count_clean,
                    uploaded_at, processing_started_at, processed_at, error_message,
                    hlr_status, hlr_batch_id, hlr_started_at, hlr_completed_at,
                    hlr_num_items, hlr_num_complete, hlr_error_message, hlr_raw_file_path
                FROM uploaded_csv_files
                ORDER BY uploaded_at DESC;
            """
            with self._cursor() as cursor:
                cursor.execute(q)
                rows = cursor.fetchall()
        return [
         {
            "id": r[0],
            "original_filename": r[1],
            "status": r[2],
            "record_count_total": r[3],
            "record_count_clean": r[4],
            "uploaded_at": r[5],
            "processing_started_at": r[6],
            "processed_at": r[7],
            "error_message": r[8],

            "hlr_status": r[9],
            "hlr_batch_id": r[10],
            "hlr_started_at": r[11],
            "hlr_completed_at": r[12],
            "hlr_num_items": r[13],
            "hlr_num_complete": r[14],
            "hlr_error_message": r[15],
            "hlr_raw_file_path": r[16],
        }
        for r in rows
        ]

    def mark_processing(self, file_id: str, lock_owner: str = None) -> bool:
        """
        Atomically set status=processing. Returns True if success.
        If another file is already processing, DB unique index will throw.
        """
        q = """
            UPDATE uploaded_csv_files
            SET status='processing',
                processing_started_at=NOW(),
                error_message=NULL,
                lock_owner=%s
            WHERE id=%s::uuid
              AND status IN ('uploaded', 'failed');
        """
        try:
            with self._cursor() as cursor:
                cursor.execute(q, (lock_owner, file_id))
                if cursor.rowcount != 1:
                    self.connection.rollback()
                    return False
            # The unique index one_processing_file_only enforces exclusivity
            self.connection.commit()
            return True
        except psycopg2.Error:
            self.connection.rollback()
            raise

    def mark_failed(self, file_id: str, error_message: str):
        q = """
            UPDATE uploaded_csv_files
            SET status='failed',
                error_message=%s,
                lock_owner=NULL
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (error_message, file_id))
        self.connection.commit()

    def mark_processed(self, file_id: str, processed_path: str, total: int, clean: int):
        q = """
            UPDATE uploaded_csv_files
            SET status='processed',
                processed_file_path=%s,
                record_count_total=%s,
                record_count_clean=%s,
                processed_at=NOW(),
                lock_owner=NULL
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (processed_path, total, clean, file_id))
        self.connection.commit()

    def archive_file(self, file_id: str):
        q = """
            UPDATE uploaded_csv_files
            SET status='archived'
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
        self.connection.commit()

    def delete_file_entry(self, file_id: str):
        q = """
            DELETE FROM uploaded_csv_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
        self.connection.commit()

    def upsert_stats(self, file_id: str, total: int, rb: int, rm: int, rc: int, rmd: int, final_clean: int):
        q = """
            INSERT INTO csv_processing_stats
              (file_id, total_records, removed_blacklist, removed_monitor, removed_conversion, removed_maindatabase, final_clean_count)
            VALUES
              (%s::uuid, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (file_id) DO UPDATE SET
              total_records=EXCLUDED.total_records,
              removed_blacklist=EXCLUDED.removed_blacklist,
              removed_monitor=EXCLUDED.removed_monitor,
              removed_conversion=EXCLUDED.removed_conversion,
              removed_maindatabase=EXCLUDED.removed_maindatabase,
              final_clean_count=EXCLUDED.final_clean_count,
              created_at=NOW();
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id, total, rb, rm, rc, rmd, final_clean))
        self.connection.commit()

    def get_stats(self, file_id: str):
        q = """
            SELECT file_id::text AS file_id, total_records, removed_blacklist, removed_monitor,
                   removed_conversion, removed_maindatabase, final_clean_count, created_at
            FROM csv_processing_stats
            WHERE file_id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
            row = cursor.fetchone()
        return {
            "file_id": row[0],
            "total_records": row[1],
            "removed_blacklist": row[2],
            "removed_monitor": row[3],
            "removed_conversion": row[4],
            "removed_maindatabase": row[5],
            "final_clean_count": row[6],
            "created_at": row[7]
        }

    def get_file_paths(self, file_id: str):
        q = """
            SELECT id::text AS id, raw_file_path, processed_file_path, status, original_filename,
                hlr_status, hlr_batch_id, hlr_result_file_path, hlr_started_at, hlr_completed_at,
                hlr_num_items, hlr_num_complete, hlr_error_message, hlr_raw_file_path
            FROM uploaded_csv_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
            r = cursor.fetchone()
        if not r:
            return None
        return {
            "id": r[0],
            "raw_file_path": r[1],
            "processed_file_path": r[2],
            "status": r[3],
            "original_filename": r[4],
            "hlr_status": r[5],
            "hlr_batch_id": r[6],
            "hlr_result_file_path": r[7],
            "hlr_started_at": r[8],
            "hlr_completed_at": r[9],
            "hlr_num_items": r[10],
            "hlr_num_complete": r[11],
            "hlr_error_message": r[12],
            "hlr_raw_file_path": r[13]
        }

    def create_manual_update_import(
        self,
        import_id: str,
        original_filename: str,
        target: str,
        raw_file_path: str,
        result_file_path: str,
        uploaded_count: int,
        accepted_count: int,
        rejected_count: int,
    ):
        q = """
            INSERT INTO manual_update_imports
            (id, original_filename, target, raw_file_path, result_file_path,
            uploaded_count, accepted_count, rejected_count, created_at)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        with self._cursor() as cursor:
            cursor.execute(q, (
                import_id, original_filename, target, raw_file_path, result_file_path,
                uploaded_count, accepted_count, rejected_count
            ))
        self.connection.commit()

    def list_manual_update_imports(self, limit: int = 50):
        q = """
            SELECT
            id::text,
            original_filename,
            target,
            uploaded_count,
            accepted_count,
            rejected_count,
            created_at
            FROM manual_update_imports
            ORDER BY created_at DESC
            LIMIT %s;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (limit,))
            rows = cursor.fetchall()
        return [
            {
                "id": r[0],
                "original_filename": r[1],
                "target": r[2],
                "uploaded_count": r[3],
                "accepted_count": r[4],
                "rejected_count": r[5],
                "created_at": r[6],
            }
            for r in rows
        ]

    def get_manual_update_import(self, import_id: str):
        q = """
            SELECT id::text, original_filename, target, raw_file_path, result_file_path
            FROM manual_update_imports
            WHERE id = %s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (import_id,))
            r = cursor.fetchone()
        if not r:
            return None
        return {
            "id": r[0],
            "original_filename": r[1],
            "target": r[2],
            "raw_file_path": r[3],
            "result_file_path": r[4],
        }

    def _get_existing_keys_any_table(self, keys: list[str], table_name: str) -> set[str]:
        if not keys:
            return set()
        existing = set()
        for i in range(0, len(keys), self.batch_size):
            chunk = keys[i:i+self.batch_size]
            q = f'SELECT custom_variable_1 FROM public."{table_name}" WHERE custom_variable_1 = ANY(%s)'
            with self._cursor() as cursor:
                cursor.execute(q, (chunk,))
                existing |= {row[0] for row in cursor.fetchall()}
        return existing

    def manual_update_insert_dedup(self, target: str, rows: list[dict]):
        """
        Inserts into target table ONLY if phone doesn't already exist in blacklist/monitor.
        Returns (inserted_set, duplicates_set)
        """
        phones = [r["phone"] for r in rows if r.get("phone")]
        phones = [str(p).strip() for p in phones if str(p).strip()]
        if not phones:
            return set(), set()

        # duplicates across both tables
        existing_blacklist = self._get_existing_keys_any_table(phones, "blacklist")
        existing_monitor = self._get_existing_keys_any_table(phones, "monitor")
        existing_any = existing_blacklist | existing_monitor
        # existing_any.extend(existing_monitor)
        to_insert = []
        duplicates = set()

        for r in rows:
            phone = str(r.get("phone", "")).strip()
            if not phone:
                continue
            if phone in existing_any:
                duplicates.add(phone)
                continue

            # map optional fields to your columns:
            # - os_version -> os_version
            # - user_agent -> browser_version (best match given your schema)
            # - ip -> ip
            to_insert.append((
                phone,
                r.get("reason"),
                r.get("source"),
                r.get("os_version"),
                r.get("user_agent"),
                r.get("ip"),
                datetime.now()
            ))

        if not to_insert:
            return set(), duplicates

        # Insert new only
        # NOTE: browser_version used for user_agent due to existing schema
        q = f"""
            INSERT INTO public."{target}"
            (custom_variable_1, reason, source, os_version, browser_version, ip, timestamp_created)
            VALUES %s
            ON CONFLICT (custom_variable_1) DO NOTHING
            RETURNING custom_variable_1;
        """

        with self._cursor() as cursor:
            execute_values(cursor, q, to_insert, page_size=self.batch_size)
            inserted_rows = cursor.fetchall()
        self.connection.commit()

        inserted = {r[0] for r in inserted_rows}
        return inserted, duplicates

    def mark_hlr_processing(self, file_id: str, lock_owner: str = "hlr") -> bool:
        """
        Automically mark HLR as starting. Enforced one-at-a-time by partial unique index.
        Only allowed when file is already processed.
        """
        q = """
            UPDATE uploaded_csv_files
            SET hlr_status='uploading',
                hlr_started_at=NOW(),
                hlr_completed_at=NULL,
                hlr_error_message=NULL,
                hlr_batch_id=NULL,
                hlr_result_file_path=NULL,
                hlr_num_items=NULL,
                hlr_num_complete=NULL,
                lock_owner=%s
            WHERE id=%s::uuid
            AND status='processed'
            AND (hlr_status IS NULL OR hlr_status IN ('failed','complete'));
        """
        try:
            with self._cursor() as cursor:
                cursor.execute(q, (lock_owner, file_id))
                if cursor.rowcount != 1:
                    self.connection.rollback()
                    return False
            self.connection.commit()
            return True
        except psycopg2.Error:
            self.connection.rollback()
            raise

    def set_hlr_batch_id(self, file_id: str, batch_id: str):
        q = """
            UPDATE uploaded_csv_files
            SET hlr_batch_id=%s
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (batch_id, file_id))
        self.connection.commit()

    def set_hlr_status(self, file_id: str, status: str):
        q = """
            UPDATE uploaded_csv_files
            SET hlr_status=%s
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (status, file_id))
        self.connection.commit()

    def update_hlr_progress(self, file_id: str, hlr_status: str, num_items: int | None, num_complete: int | None):
        q = """
            UPDATE uploaded_csv_files
            SET hlr_status=%s,
                hlr_num_items=%s,
                hlr_num_complete=%s
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (hlr_status, num_items, num_complete, file_id))
        self.connection.commit()

    def mark_hlr_complete(self, file_id: str, result_path: str):
        q = """
            UPDATE uploaded_csv_files
            SET hlr_status='complete',
                hlr_result_file_path=%s,
                hlr_completed_at=NOW(),
                lock_owner=NULL
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (result_path, file_id))
        self.connection.commit()

    def mark_hlr_failed(self, file_id: str, error_message: str):
        q = """
            UPDATE uploaded_csv_files
            SET hlr_status='failed',
                hlr_error_message=%s,
                lock_owner=NULL
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (error_message, file_id))
        self.connection.commit()

    def get_hlr_info(self, file_id: str):
        q = """
            SELECT id::text, hlr_status, hlr_batch_id, hlr_result_file_path,
                hlr_started_at, hlr_completed_at, hlr_num_items, hlr_num_complete, hlr_error_message, hlr_raw_file_path
            FROM uploaded_csv_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
            r = cursor.fetchone()
        if not r:
            return None
        return {
            "file_id": r[0],
            "hlr_status": r[1],
            "hlr_batch_id": r[2],
            "hlr_result_file_path": r[3],
            "hlr_started_at": r[4],
            "hlr_completed_at": r[5],
            "hlr_num_items": r[6],
            "hlr_num_complete": r[7],
            "hlr_error_message": r[8],
            "hlr_raw_file_path": r[9]
        }

    def insert_hlr_data(self, table_name, numbers, source, reason):
        """
        Bulk insert into table.
        Duplicate-safe.
        """
        if not numbers:
            return

        q = f"""
            INSERT INTO public."{table_name}" ( custom_variable_1, timestamp_created, source, reason)
            VALUES %s
            ON CONFLICT (custom_variable_1) DO NOTHING;
        """
        now = datetime.now()
        rows = []

        for record in numbers:
            rows.append((
                int(record), now, source, reason
            ))
        with self._cursor() as cursor:
            execute_values(cursor, q, rows, page_size=50000)
        self.connection.commit()

    def set_hlr_raw_path(self, file_id: str, path: str):
        q = """
            UPDATE uploaded_csv_files
            SET hlr_raw_file_path=%s
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (path, file_id))
        self.connection.commit()

    def insert_raw_hlr_data(self, payloads):
        """
        Insert HLR live numbers into api_hlr_live_numbers.
        Accepts list of dicts with dotted keys.
        Uses ON CONFLICT DO NOTHING.
        """

        if not payloads:
            return

        # Convert to DataFrame
        df = pd.DataFrame(payloads)
        if "custom_variable_1" not in df.columns:
            first_col = df.columns[0]
            df = df.rename(columns={first_col: "custom_variable_1"})

        column_map = {
            "custom_variable_1": "custom_variable_1",
            "error": "error",

            "request_parameters.telephone_number": "request_parameters_telephone_number",
            "request_parameters.cache_days_private": "request_parameters_cache_days_private",
            "request_parameters.cache_days_global": "request_parameters_cache_days_global",
            "request_parameters.save_to_cache": "request_parameters_save_to_cache",
            "request_parameters.output_format": "request_parameters_output_format",

            "credits_spent": "credits_spent",

            "detected_telephone_number": "detected_telephone_number",
            "formatted_telephone_number": "formatted_telephone_number",
            "telephone_number_type": "telephone_number_type",

            "live_status": "live_status",

            "original_network": "original_network",
            "original_network_details.name": "original_network_details_name",
            "original_network_details.mccmnc": "original_network_details_mccmnc",
            "original_network_details.country_name": "original_network_details_country_name",
            "original_network_details.country_iso3": "original_network_details_country_iso3",
            "original_network_details.area": "original_network_details_area",
            "original_network_details.country_prefix": "original_network_details_country_prefix",

            "current_network": "current_network",
            "current_network_details.name": "current_network_details_name",
            "current_network_details.mccmnc": "current_network_details_mccmnc",
            "current_network_details.country_name": "current_network_details_country_name",
            "current_network_details.country_iso3": "current_network_details_country_iso3",
            "current_network_details.country_prefix": "current_network_details_country_prefix",

            "is_ported": "is_ported",
            "timestamp": "timestamp",
        }

        df = df.rename(columns=column_map)

        # Ensure all DB columns exist
        for col in column_map.values():
            if col not in df.columns:
                df[col] = None

        df = df[list(column_map.values())]
        df = df.where(pd.notnull(df), None)

        columns = df.columns.tolist()
        values = df.values.tolist()

        query = f"""
            INSERT INTO api_hlr_live_numbers ({", ".join(columns)})
            VALUES %s
            ON CONFLICT (custom_variable_1) DO NOTHING
        """

        with self._cursor() as cur:
            execute_values(cur, query, values, page_size=1000)

        self.connection.commit()

    def create_db_entry(self, table_name: str, id: str, file_path: str):
        """
        Create a DB Export file entry.
        """
        try:
            q = f"""
                INSERT INTO db_exports (id, table_name, file_path, status)
                VALUES (%s::uuid, %s, %s, 'pending');
            """
            with self._cursor() as cursor:
                cursor.execute(q, (id, table_name, file_path))
            self.connection.commit()
            print(f"Created DB export entry for table {table_name} with id {id}.")
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Failed to create DB export entry: {e}")
            raise

    def get_db_exports_path(self, file_id: str):
        """
        Get DB Export file paths.
        """
        q = """
            SELECT id::text AS id, file_path, status, table_name
            FROM db_exports
            WHERE id=%s::uuid;
        """

        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
            r = cursor.fetchone()
        if not r:
            return None
        return {
            "id": r[0],
            "file_path": r[1],
            "status": r[2],
            "table_name": r[3]
        }

    def export_db_to_csv(self, table_name: str, file_path: str):
        query = f"""
            COPY "{table_name}"
            TO STDOUT WITH CSV HEADER
        """
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                with self._cursor() as cursor:
                    cursor.copy_expert(query, f)

            print(f"✅ Exported table using COPY to {file_path}")
            return True
        except Exception as e:
            print(f"❌ Failed to export table using COPY: {e}")
            return False

    def upsert_db_export_status(self, file_id: str, status: str):
        """
        Update DB Export file status.
        """
        q = """
            UPDATE db_exports
            SET status=%s, completed_at=NOW()
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (status, file_id))
        self.connection.commit()

    def fetch_latest_db_export(self, table_name: str):
        """
        Fetch the latest DB Export entry for a given table.
        """
        q = """
            SELECT id::text AS id, file_path, status, created_at, completed_at
            FROM db_exports
            WHERE table_name=%s
            ORDER BY created_at DESC
            LIMIT 1;
        """

        with self._cursor() as cursor:
            cursor.execute(q, (table_name,))
            r = cursor.fetchone()
        if not r:
            return None
        return {
            "id": r[0],
            "file_path": r[1],
            "status": r[2],
            "created_at": r[3],
            "completed_at": r[4]
        }

    def create_encrypted_file_entry(self, file_id, original_filename: str, raw_file_path: str) -> str:
        """
        Insert a new file entry and return its UUID (as str).
        """
        q = """
            INSERT INTO uploaded_encryption_files (id, original_filename, raw_file_path, status)
            VALUES (%s::uuid, %s, %s, 'uploaded')
            RETURNING id::text AS id;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id, original_filename, raw_file_path))
            created = cursor.fetchone()
        self.connection.commit()
        return {"id": created[0], "original_filename": original_filename, "status": "uploaded"}

    def list_encrypted_files(self, statuses=None):
        if statuses:
            q = """
                SELECT id::text AS id, original_filename, status,
                    uploaded_at, processing_started_at, processed_at, error_message
                FROM uploaded_encryption_files
                WHERE status = ANY(%s)
                ORDER BY uploaded_at DESC;
            """
            with self._cursor() as cursor:
                cursor.execute(q, (statuses,))
                rows = cursor.fetchall()
        else:
            q = """
                SELECT id::text AS id, original_filename, status,
                    uploaded_at, processing_started_at, processed_at, error_message
                FROM uploaded_csv_files
                ORDER BY uploaded_at DESC;
            """
            with self._cursor() as cursor:
                cursor.execute(q)
                rows = cursor.fetchall()
        return [
         {
            "id": r[0],
            "original_filename": r[1],
            "status": r[2],
            "uploaded_at": r[3],
            "processing_started_at": r[4],
            "processed_at": r[5],
            "error_message": r[6],
        }
        for r in rows
        ]

    def mark_encrypted_processing(self, file_id: str) -> bool:
        """
        Atomically set status=processing. Returns True if success.
        If another file is already processing, DB unique index will throw.
        """
        q = """
            UPDATE uploaded_encryption_files
            SET status='processing',
                processing_started_at=NOW(),
                error_message=NULL
            WHERE id=%s::uuid
            AND status IN ('uploaded', 'failed');
        """
        try:
            with self._cursor() as cursor:
                cursor.execute(q, (file_id,))
                if cursor.rowcount != 1:
                    self.connection.rollback()
                    return False
            # The unique index one_processing_file_only enforces exclusivity
            self.connection.commit()
            return True
        except psycopg2.Error as e:
            self.connection.rollback()
            raise

    def get_encrypted_file_paths(self, file_id: str):
        q = """
            SELECT id::text AS id, raw_file_path, encrypted_file_path, status, original_filename
            FROM uploaded_encryption_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
            r = cursor.fetchone()
        if not r:
            return None
        return {
            "id": r[0],
            "raw_file_path": r[1],
            "encrypted_file_path": r[2],
            "status": r[3],
            "original_filename": r[4],
        }

    def mark_encrypted_processed(self, file_id: str, processed_path: str):
        q = """
            UPDATE uploaded_encryption_files
            SET status='processed',
                encrypted_file_path=%s,
                processed_at=NOW()
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (processed_path, file_id))
        self.connection.commit()

    def mark_encrypted_failed(self, file_id: str, error_message: str):
        q = """
            UPDATE uploaded_encryption_files
            SET status='failed',
                error_message=%s
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (error_message, file_id))
        self.connection.commit()

    def archive_encrypted_file(self, file_id: str):
        q = """
            UPDATE uploaded_encryption_files
            SET status='archived'
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
        self.connection.commit()

    def delete_encrypted_file_entry(self, file_id: str):
        q = """
            DELETE FROM uploaded_encryption_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
        self.connection.commit()

    def create_broadcasts_file_entry(self, file_id, filename: str, raw_file_path: str, data_date: str, number_of_broadcasts: int) -> bool:
        """
        Insert a new file entry and return its UUID (as str).
        """
        q = """
            INSERT INTO api_mmd_broadcasts_files (id, filename, raw_file_path, data_date, number_of_broadcasts)
            VALUES (%s::uuid, %s, %s, %s, %s)
            RETURNING id::text AS id;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id, filename, raw_file_path, data_date, number_of_broadcasts))
            created = cursor.fetchone()
        self.connection.commit()
        return

    def upsert_broadcast_campaign_stats(self, df):
        """
        Insert / update broadcast campaign stats from a pandas DataFrame
        using execute_values for high performance.
        """

        def remove_percentage(value: str):
            return value.replace("%", "")


        if df.empty:
            return

        # Replace NaN with None so PostgreSQL accepts NULLs
        df = df.where(df.notna(), None)

        COLUMN_MAPPING = {
            "Campaign ID": "campaign_id",
            "Broadcast ID": "broadcast_id",
            "Name": "name",
            "Send Date": "send_date",
            "Estimated Price": "estimated_price",
            "Recipient Count": "recipient_count",
            "BC Unique Clicks": "bc_unique_clicks",
            "BC Total Clicks": "bc_total_clicks",
            "CTR": "ctr",
            "DLR Delivered Count": "dlr_delivered_count",
            "DLR Sent Count": "dlr_sent_count",
            "DLR Undelivered Count": "dlr_undelivered_count",
            "DLR Rejected Count": "dlr_rejected_count",
            "Voluum Visits": "voluum_visits",
            "Voluum Unique Visits": "voluum_unique_visits",
            "Click 2 REG": "click_2_reg",
            "Reg 2 FTD": "reg_2_ftd",
            "message_body": "message_body",
        }

        df = df.rename(columns=COLUMN_MAPPING)
        df["ctr"] = df["ctr"].apply(remove_percentage)
        df["click_2_reg"] = df["click_2_reg"].apply(remove_percentage)
        df["reg_2_ftd"] = df["reg_2_ftd"].apply(remove_percentage)

        columns = [
            "campaign_id",
            "broadcast_id",
            "name",
            "send_date",
            "estimated_price",
            "recipient_count",
            "bc_unique_clicks",
            "bc_total_clicks",
            "ctr",
            "dlr_delivered_count",
            "dlr_sent_count",
            "dlr_undelivered_count",
            "dlr_rejected_count",
            "voluum_visits",
            "voluum_unique_visits",
            "click_2_reg",
            "reg_2_ftd",
            "message_body",
        ]

        values = df[columns].to_records(index=False).tolist()

        sql_q = f"""
            INSERT INTO broadcast_campaign_stats (
                {", ".join(columns)}
            )
            VALUES %s
            ON CONFLICT (broadcast_id)
            DO UPDATE SET
                campaign_id = EXCLUDED.campaign_id,
                name = EXCLUDED.name,
                send_date = EXCLUDED.send_date,
                estimated_price = EXCLUDED.estimated_price,
                recipient_count = EXCLUDED.recipient_count,
                bc_unique_clicks = EXCLUDED.bc_unique_clicks,
                bc_total_clicks = EXCLUDED.bc_total_clicks,
                ctr = EXCLUDED.ctr,
                dlr_delivered_count = EXCLUDED.dlr_delivered_count,
                dlr_sent_count = EXCLUDED.dlr_sent_count,
                dlr_undelivered_count = EXCLUDED.dlr_undelivered_count,
                dlr_rejected_count = EXCLUDED.dlr_rejected_count,
                voluum_visits = EXCLUDED.voluum_visits,
                voluum_unique_visits = EXCLUDED.voluum_unique_visits,
                click_2_reg = EXCLUDED.click_2_reg,
                reg_2_ftd = EXCLUDED.reg_2_ftd,
                message_body = EXCLUDED.message_body
        """

        with self._cursor() as cursor:
            execute_values(cursor, sql_q, values, page_size=self.batch_size)
        self.connection.commit()

    def list_broadcasts(self):
        q = """
                SELECT id::text AS id, filename,
                    uploaded_at, data_date, number_of_broadcasts
                FROM api_mmd_broadcasts_files
                ORDER BY uploaded_at DESC;
            """
        with self._cursor() as cursor:
            cursor.execute(q)
            rows = cursor.fetchall()
        return [
         {
            "id": r[0],
            "filename": r[1],
            "uploaded_at": r[2],
            "data_date": r[3],
            "number_of_broadcasts": r[4]
        }
        for r in rows
        ]

    def get_broadcast_history_file_path(self, file_id: str):
        q = """
            SELECT id::text AS id, raw_file_path, filename
            FROM api_mmd_broadcasts_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
            r = cursor.fetchone()
        if not r:
            return None
        return {
            "id": r[0],
            "raw_file_path": r[1],
            "filename": r[2]
        }

    def create_csv_job(self,job_id: str,original_filename: str,interval_seconds: int,lower_limit: int,upper_limit: int):
        print("Here")
        q = """
            INSERT INTO csv_jobs (
                id,
                original_filename,
                interval_seconds,
                lower_limit,
                upper_limit
            )
            VALUES (%s, %s, %s, %s, %s)
        """
        with self._cursor() as cursor:
            cursor.execute(q, (job_id, original_filename, interval_seconds, lower_limit, upper_limit))
        self.connection.commit()

    def insert_csv_active_records(self, rows: list[tuple]) -> int:
        print("Hi")
        q = """
            INSERT INTO csv_active_records (
                file_id,
                list_id,
                email,
                country,
                source,
                mobile,
                conversion_type,
                original_source,
                hlr_status,
                uuid
            )
            VALUES %s
        """
        with self._cursor() as cursor:
            execute_values(cursor, q, rows, page_size=self.batch_size)
        self.connection.commit()
        return len(rows)

    def update_csv_job_total_records(self, job_id: str, total_records: int):
        print("Hello")
        q = """
            UPDATE csv_jobs
            SET total_records = %s
            WHERE id = %s
        """
        with self._cursor() as cursor:
            cursor.execute(q, (total_records, job_id))
        self.connection.commit()

    def list_all_csv_ongage_files(self):
        try:
            q = """
                SELECT id, original_filename, status, total_records, uploaded_at,
                        interval_seconds, lower_limit, upper_limit
                FROM csv_jobs
                WHERE status != 'archived'
                ORDER BY uploaded_at DESC
            """
            with self._cursor() as cursor:
                cursor.execute(q)
                rows = cursor.fetchall()
            return {"items": [{
                "id": row[0],
                "original_filename": row[1],
                "status": row[2],
                "total_records": row[3],
                "uploaded_at": row[4],
                "interval_seconds": row[5],
                "lower_limit": row[6],
                "upper_limit": row[7]
            } for row in rows]}
        except Exception as e:
            return None

    def update_ongage_csv_config(self, interval_seconds, lower_limit, upper_limit, job_id):
        try:
            with self._cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE csv_jobs
                    SET
                        interval_seconds = %s,
                        lower_limit = %s,
                        upper_limit = %s
                    WHERE id = %s
                    """,
                    (interval_seconds, lower_limit, upper_limit, job_id),
                )
            self.connection.commit()
            return {"ok": True, "job_id": job_id}
        except Exception as e:
            return e

    def start_ongage_csv_job(self, job_id):
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE csv_jobs
                SET
                    status = 'running',
                    next_run_at = now()
                WHERE id = %s
                  AND status IN ('draft','paused')
                """,
                (job_id,)
            )
            rowcount = cursor.rowcount
        self.connection.commit()
        return rowcount

    def pause_ongage_csv_job(self, job_id):
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE csv_jobs
                SET status = 'paused'
                WHERE id = %s
                  AND status = 'running'
                """,
                (job_id,)
            )
            rowcount = cursor.rowcount
        self.connection.commit()
        return rowcount

    def resume_ongage_csv_job(self, job_id):
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE csv_jobs
                SET
                    status = 'running',
                    next_run_at = now()
                WHERE id = %s
                  AND status = 'paused'
                """,
                (job_id,)
            )
            rowcount = cursor.rowcount
        self.connection.commit()
        return rowcount

    def find_clickers_based_on_country_code(self, country_code, from_date=None, to_date=None):
        q = """
            SELECT custom_variable_1, timestamp_created from whitelist
            where country_code = %s
        """
        params = [country_code]
        if from_date:
            q += " AND timestamp_created >= %s"
            params.append(from_date + " 00:00:00")
        if to_date:
            q += " AND timestamp_created <= %s"
            params.append(to_date + " 23:59:59")
        with self._cursor() as cursor:
            cursor.execute(q, tuple(params))
            return cursor.fetchall()

    def create_smart_cleaning_voluum_file_entry(self, file_id, raw_path, original_filename, country_code, from_date=None, to_date=None) -> str:
        """
        Insert a new file entry and return its UUID (as str).
        """
        clickers = self.find_clickers_based_on_country_code(country_code, from_date, to_date)
        if not clickers:
            return False
        raw_file_path = Path(raw_path)
        with raw_file_path.open(mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            for row in clickers:
                formatted_row = []

                for value in row:
                    formatted_row.append(value)

                writer.writerow(formatted_row)

        q = """
            select distinct offer_name
            from api_voluum_conversions avc
            where avc.country_code = %s
        """
        with self._cursor() as cursor:
            cursor.execute(q, (country_code, ))
            offers = cursor.fetchall()
        offer_count = len(offers)
        offers = "^".join([offer[0] for offer in offers])
        q = """
            INSERT INTO uploaded_smart_cleaning_voluum_files (id, original_filename, raw_file_path, status, record_count_total, offer_count, offers, from_date, to_date)
            VALUES (%s::uuid, %s, %s, 'uploaded', %s, %s, %s, %s, %s)
            RETURNING id::text AS id;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id, original_filename, raw_path, len(clickers), offer_count, offers, from_date, to_date))
            created = cursor.fetchone()
        self.connection.commit()
        return {"id": created[0], "original_filename": original_filename, "status": "uploaded"}

    def create_smart_cleaning_voluum_file_entry_from_upload(self, file_id, raw_path, original_filename, row_count):
        """
        Create a file entry from a user-uploaded CSV file.
        Gets all distinct offers from api_voluum_conversions.
        """
        q = """
            SELECT DISTINCT offer_name
            FROM api_voluum_conversions
            WHERE offer_name IS NOT NULL
        """
        with self._cursor() as cursor:
            cursor.execute(q)
            offers = cursor.fetchall()
        if not offers:
            return False
        offer_count = len(offers)
        offers_str = "^".join([offer[0] for offer in offers])
        q = """
            INSERT INTO uploaded_smart_cleaning_voluum_files (id, original_filename, raw_file_path, status, record_count_total, offer_count, offers)
            VALUES (%s::uuid, %s, %s, 'uploaded', %s, %s, %s)
            RETURNING id::text AS id;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id, original_filename, raw_path, row_count, offer_count, offers_str))
            created = cursor.fetchone()
        self.connection.commit()
        return {"id": created[0], "original_filename": original_filename, "status": "uploaded"}

    def list_smart_cleaning_voluum_files(self, statuses=None):
        if statuses:
            q = """
                SELECT id::text AS id, original_filename, status,
                    uploaded_at, processing_started_at, processed_at, error_message, record_count_total, record_count_clean, offer_count, offers, selected_offer
                FROM uploaded_smart_cleaning_voluum_files
                WHERE status = ANY(%s)
                ORDER BY uploaded_at DESC;
            """
            with self._cursor() as cursor:
                cursor.execute(q, (statuses,))
                rows = cursor.fetchall()
        else:
            q = """
                SELECT id::text AS id, original_filename, status,
                    uploaded_at, processing_started_at, processed_at, error_message, record_count_total, record_count_clean, offer_count, offers, selected_offer
                FROM uploaded_smart_cleaning_voluum_files
                ORDER BY uploaded_at DESC;
            """
            with self._cursor() as cursor:
                cursor.execute(q)
                rows = cursor.fetchall()
        return [
         {
            "id": r[0],
            "original_filename": r[1],
            "status": r[2],
            "uploaded_at": r[3],
            "processing_started_at": r[4],
            "processed_at": r[5],
            "error_message": r[6],
            "record_count_total": r[7],
            "record_count_clean": r[8],
            "offer_count": r[9],
            "offers": r[10],
            "selected_offer": r[11]
        }
        for r in rows
        ]

    def mark_smart_cleaning_files_voluum_processing(self, file_id):
        """
        Atomically set status=processing. Returns True if success.
        If another file is already processing, DB unique index will throw.
        """
        q = """
            UPDATE uploaded_smart_cleaning_voluum_files
            SET status='processing',
                processing_started_at=NOW(),
                error_message=NULL
            WHERE id=%s::uuid
            AND status IN ('uploaded', 'failed', 'processed');
        """
        try:
            with self._cursor() as cursor:
                cursor.execute(q, (file_id,))
                if cursor.rowcount != 1:
                    self.connection.rollback()
                    return False
            # The unique index one_processing_file_only enforces exclusivity
            self.connection.commit()
            return True
        except psycopg2.Error as e:
            self.connection.rollback()
            raise

    def get_smart_cleaning_files_voluum_file_paths(self, file_id: str):
        q = """
            SELECT id::text AS id, raw_file_path, cleaned_file_path, status, original_filename, from_date, to_date
            FROM uploaded_smart_cleaning_voluum_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
            r = cursor.fetchone()
        if not r:
            return None
        return {
            "id": r[0],
            "raw_file_path": r[1],
            "cleaned_file_path": r[2],
            "status": r[3],
            "original_filename": r[4],
            "from_date": r[5],
            "to_date": r[6],
        }

    def mark_smart_cleaning_files_voluum_processed(self, file_id: str, processed_path: str, record_count: int, offer_name: str):
        q = """
            UPDATE uploaded_smart_cleaning_voluum_files
            SET status='processed',
                cleaned_file_path=%s,
                processed_at=NOW(),
                record_count_clean=%s,
                selected_offer=%s
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (processed_path, record_count, offer_name, file_id))
        self.connection.commit()

    def mark_smart_cleaning_files_voluum_failed(self, file_id: str, error_message: str):
        q = """
            UPDATE uploaded_smart_cleaning_voluum_files
            SET status='failed',
                error_message=%s
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (error_message, file_id))
        self.connection.commit()

    def archive_smart_cleaning_voluum_file(self, file_id: str):
        q = """
            UPDATE uploaded_smart_cleaning_voluum_files
            SET status='archived'
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
        self.connection.commit()

    def delete_smart_cleaning_voluum_file_entry(self, file_id: str):
        q = """
            DELETE FROM uploaded_smart_cleaning_voluum_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
        self.connection.commit()

    def get_live_events_last_24h(self, offer_names: list[str]) -> set[str]:
        """
        Return the set of custom_variable_1 values from voluum_live_events
        where the offer_name matches any of the given offer_names
        and the event timestamp is within the last 24 hours.
        """
        if not offer_names:
            return set()

        q = """
            SELECT DISTINCT custom_variable_1
            FROM public.voluum_live_events
            WHERE offer_name = ANY(%s)
              AND timestamp >= NOW() - INTERVAL '24 hours'
              AND custom_variable_1 IS NOT NULL
        """
        with self._cursor() as cursor:
            cursor.execute(q, (offer_names,))
            rows = cursor.fetchall()
        result = {r[0] for r in rows}
        print(f"Found {len(result)} phone numbers in live events (last 24h) for offers: {offer_names}")
        return result

    def filter_phone_numbers_offers(self, keys: list[str], offer_name, filter_regs: bool = False) -> list[str]:
        """
        Remove keys that already have an FTD conversion for the given offer.
        If filter_regs is True, also remove keys that have a REG conversion for the offer.

        Instead of sending clicker numbers to the DB in batches, this fetches
        all FTD numbers for the offer in a single query and subtracts them
        in Python — avoids large ANY() comparisons on the DB side.

        Args:
            keys: list of custom_variable_1 values (clicker phone numbers)
            offer_name: the offer to check conversions against
            filter_regs: if True, also filter out numbers with REG conversions

        Returns:
            list of keys NOT in the conversions table for this offer
        """
        if not keys:
            return []

        remaining_keys = set(keys)

        # Single query: get all FTD phone numbers for this offer
        query = """
            SELECT DISTINCT custom_variable_1
            FROM public."api_voluum_conversions"
            WHERE offer_name = %s
                AND conversion_type = 'FTD'
                AND custom_variable_1 IS NOT NULL
        """
        with self._cursor() as cursor:
            cursor.execute(query, (offer_name,))
            ftds = cursor.fetchall()
        print(ftds)
        ftd_numbers = {row[0] for row in ftds}

        # Subtract FTD numbers from clickers
        remaining_keys -= ftd_numbers
        print(f"Found {len(ftd_numbers)} FTD numbers for offer '{offer_name}'. {len(remaining_keys)} keys remain after filtering.")

        # If filter_regs is enabled, also remove REG conversions for this offer
        if filter_regs:
            reg_query = """
                SELECT DISTINCT custom_variable_1
                FROM public."api_voluum_conversions"
                WHERE offer_name = %s
                    AND conversion_type = 'REG'
                    AND custom_variable_1 IS NOT NULL
            """
            with self._cursor() as cursor:
                cursor.execute(reg_query, (offer_name,))
                regs = cursor.fetchall()
            reg_numbers = {row[0] for row in regs}
            remaining_keys -= reg_numbers
            print(f"Found {len(reg_numbers)} REG numbers for offer '{offer_name}'. {len(remaining_keys)} keys remain after REG filtering.")

        return list(remaining_keys)


    # ============================================================
    # REG Search (uploaded_reg_search_files)
    # ============================================================

    def create_reg_search_entry(self, file_id, original_filename, country_code, from_date=None, to_date=None):
        """
        Create a reg search entry for a country code.
        Fetches distinct offers for that country from api_voluum_conversions.
        """
        q = """
            SELECT DISTINCT offer_name
            FROM api_voluum_conversions
            WHERE country_code = %s
              AND offer_name IS NOT NULL
        """
        params = [country_code]
        if from_date:
            q += " AND postback_timestamp >= %s"
            params.append(from_date + " 00:00:00")
        if to_date:
            q += " AND postback_timestamp <= %s"
            params.append(to_date + " 23:59:59")
        with self._cursor() as cursor:
            cursor.execute(q, tuple(params))
            offers = cursor.fetchall()
        if not offers:
            return False
        offer_count = len(offers)
        offers_str = "^".join([offer[0] for offer in offers])

        q = """
            INSERT INTO uploaded_reg_search_files
                (id, original_filename, country_code, status, offer_count, offers, from_date, to_date)
            VALUES (%s::uuid, %s, %s, 'uploaded', %s, %s, %s, %s)
            RETURNING id::text AS id;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id, original_filename, country_code, offer_count, offers_str, from_date, to_date))
            created = cursor.fetchone()
        self.connection.commit()
        return {
            "id": created[0],
            "original_filename": original_filename,
            "country_code": country_code,
            "status": "uploaded",
            "offer_count": offer_count,
            "offers": offers_str,
        }

    def list_reg_search_files(self, statuses=None):
        if statuses:
            q = """
                SELECT id::text AS id, original_filename, country_code, status,
                       uploaded_at, processing_started_at, processed_at,
                       error_message, record_count_total, record_count_clean,
                       offer_count, offers, selected_offer
                FROM uploaded_reg_search_files
                WHERE status = ANY(%s)
                ORDER BY uploaded_at DESC;
            """
            with self._cursor() as cursor:
                cursor.execute(q, (statuses,))
                rows = cursor.fetchall()
        else:
            q = """
                SELECT id::text AS id, original_filename, country_code, status,
                       uploaded_at, processing_started_at, processed_at,
                       error_message, record_count_total, record_count_clean,
                       offer_count, offers, selected_offer
                FROM uploaded_reg_search_files
                ORDER BY uploaded_at DESC;
            """
            with self._cursor() as cursor:
                cursor.execute(q)
                rows = cursor.fetchall()
        return [
            {
                "id": r[0],
                "original_filename": r[1],
                "country_code": r[2],
                "status": r[3],
                "uploaded_at": r[4],
                "processing_started_at": r[5],
                "processed_at": r[6],
                "error_message": r[7],
                "record_count_total": r[8],
                "record_count_clean": r[9],
                "offer_count": r[10],
                "offers": r[11],
                "selected_offer": r[12],
            }
            for r in rows
        ]

    def mark_reg_search_processing(self, file_id):
        q = """
            UPDATE uploaded_reg_search_files
            SET status='processing',
                processing_started_at=NOW(),
                error_message=NULL
            WHERE id=%s::uuid
              AND status IN ('uploaded', 'failed');
        """
        try:
            with self._cursor() as cursor:
                cursor.execute(q, (file_id,))
                if cursor.rowcount != 1:
                    self.connection.rollback()
                    return False
            self.connection.commit()
            return True
        except psycopg2.Error:
            self.connection.rollback()
            raise

    def get_reg_search_meta(self, file_id):
        q = """
            SELECT id::text AS id, country_code, cleaned_file_path, status, original_filename, from_date, to_date
            FROM uploaded_reg_search_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
            r = cursor.fetchone()
        if not r:
            return None
        return {
            "id": r[0],
            "country_code": r[1],
            "cleaned_file_path": r[2],
            "status": r[3],
            "original_filename": r[4],
            "from_date": r[5],
            "to_date": r[6],
        }

    def find_reg_only_numbers(self, country_code, offer_names, from_date=None, to_date=None):
        """
        Find phone numbers that have a REG conversion but NO FTD conversion
        for the given country_code and offer_name(s).
        Supports both a single offer string or a list of offers.
        Optionally filters by postback_timestamp date range.
        """
        if isinstance(offer_names, str):
            offer_names = [offer_names]
        if not offer_names:
            return []

        date_filter = ""
        date_params = []
        if from_date:
            date_filter += " AND postback_timestamp >= %s"
            date_params.append(from_date + " 00:00:00")
        if to_date:
            date_filter += " AND postback_timestamp <= %s"
            date_params.append(to_date + " 23:59:59")

        query = f"""
            SELECT DISTINCT custom_variable_1
            FROM api_voluum_conversions
            WHERE country_code = %s
              AND offer_name = ANY(%s)
              AND conversion_type = 'REG'
              AND custom_variable_1 IS NOT NULL
              {date_filter}
              AND custom_variable_1 NOT IN (
                  SELECT DISTINCT custom_variable_1
                  FROM api_voluum_conversions
                  WHERE country_code = %s
                    AND offer_name = ANY(%s)
                    AND conversion_type = 'FTD'
                    AND custom_variable_1 IS NOT NULL
                    {date_filter}
              )
        """
        params = [country_code, offer_names] + date_params + [country_code, offer_names] + date_params
        with self._cursor() as cursor:
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
        return [r[0] for r in rows]

    def mark_reg_search_processed(self, file_id, cleaned_path, total_regs, clean_count, offer_name):
        q = """
            UPDATE uploaded_reg_search_files
            SET status='processed',
                cleaned_file_path=%s,
                processed_at=NOW(),
                record_count_total=%s,
                record_count_clean=%s,
                selected_offer=%s
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (cleaned_path, total_regs, clean_count, offer_name, file_id))
        self.connection.commit()

    def mark_reg_search_failed(self, file_id, error_message):
        q = """
            UPDATE uploaded_reg_search_files
            SET status='failed',
                error_message=%s
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (error_message, file_id))
        self.connection.commit()

    def archive_reg_search_file(self, file_id):
        q = """
            UPDATE uploaded_reg_search_files
            SET status='archived'
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
        self.connection.commit()

    def delete_reg_search_file(self, file_id):
        q = """
            DELETE FROM uploaded_reg_search_files
            WHERE id=%s::uuid;
        """
        with self._cursor() as cursor:
            cursor.execute(q, (file_id,))
        self.connection.commit()

    def import_csv_into_blacklist(self, csv_path: str):
        """
        Read a CSV file and bulk-upsert all rows into the monitor table.
        CSV columns must match the table schema.
        Returns the number of rows processed.
        """
        db_columns = [
            "custom_variable_1", "ip", "os_version", "browser_version",
            "timestamp_created", "reason", "source", "click_2_reg", "reg_2_ftd",
            "clicks", "conversions", "cost", "cost_sources", "cpv",
            "custom_conversions_1", "custom_conversions_2", "custom_conversions_3",
            "custom_revenue_1", "custom_revenue_2", "custom_revenue_3",
            "cv", "epv", "errors", "profit", "revenue", "roi",
            "suspicious_clicks", "suspicious_clicks_percentage",
            "suspicious_visits", "suspicious_visits_percentage",
            "unique_visits", "visits",
        ]

        rows = []
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                values = []
                for db_col in db_columns:
                    val = (row.get(db_col) or "").strip()
                    if val == "":
                        val = None
                    values.append(val)
                rows.append(tuple(values))

        if not rows:
            return 0

        cols_str = ", ".join(db_columns)
        update_cols = [c for c in db_columns if c != "custom_variable_1"]
        update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

        q = f"""
            INSERT INTO public."monitor" ({cols_str})
            VALUES %s
            ON CONFLICT (custom_variable_1)
            DO UPDATE SET {update_str};
        """

        with self._cursor() as cursor:
            execute_values(cursor, q, rows, page_size=self.batch_size)
        self.connection.commit()

        return len(rows)

    # ============================================================
    # Voluum Offers & Campaigns lookup tables
    # ============================================================

    def upsert_offers(self, offers: list[dict]):
        """
        Bulk upsert offers into voluum_offers.
        Each dict should have 'offerId' and 'offerName' keys (from Voluum API).
        Skips duplicates, updates offer_name if it changed.
        """
        if not offers:
            return 0

        q = """
            INSERT INTO public.voluum_offers (offer_id, offer_name, updated_at)
            VALUES %s
            ON CONFLICT (offer_id) DO UPDATE SET
                offer_name = EXCLUDED.offer_name,
                updated_at = EXCLUDED.updated_at;
        """
        now = datetime.now()
        rows = []
        for o in offers:
            offer_id = o.get("offerId") or o.get("offer_id")
            offer_name = o.get("offerName") or o.get("offer_name")
            if offer_id:
                rows.append((offer_id, offer_name, now))

        if not rows:
            return 0

        with self._cursor() as cursor:
            execute_values(cursor, q, rows, page_size=self.batch_size)
        self.connection.commit()
        print(f"Upserted {len(rows)} offers into voluum_offers.")
        return len(rows)

    def upsert_campaigns(self, campaigns: list[dict]):
        """
        Bulk upsert campaigns into voluum_campaigns.
        Each dict should have 'campaignId' and 'campaignName' keys (from Voluum API).
        """
        if not campaigns:
            return 0

        q = """
            INSERT INTO public.voluum_campaigns (campaign_id, campaign_name, updated_at)
            VALUES %s
            ON CONFLICT (campaign_id) DO UPDATE SET
                campaign_name = EXCLUDED.campaign_name,
                updated_at = EXCLUDED.updated_at;
        """
        now = datetime.now()
        rows = []
        for c in campaigns:
            campaign_id = c.get("campaignId") or c.get("campaign_id")
            campaign_name = c.get("campaignName") or c.get("campaign_name")
            if campaign_id:
                rows.append((campaign_id, campaign_name, now))

        if not rows:
            return 0

        with self._cursor() as cursor:
            execute_values(cursor, q, rows, page_size=self.batch_size)
        self.connection.commit()
        print(f"Upserted {len(rows)} campaigns into voluum_campaigns.")
        return len(rows)

    def get_offer_name(self, offer_id: str) -> str | None:
        """Lookup offer_name by offer_id. Returns None if not found."""
        if not offer_id:
            return None
        q = "SELECT offer_name FROM public.voluum_offers WHERE offer_id = %s;"
        with self._cursor() as cursor:
            cursor.execute(q, (offer_id,))
            r = cursor.fetchone()
        return r[0] if r else None

    def get_campaign_name(self, campaign_id: str) -> str | None:
        """Lookup campaign_name by campaign_id. Returns None if not found."""
        if not campaign_id:
            return None
        q = "SELECT campaign_name FROM public.voluum_campaigns WHERE campaign_id = %s;"
        with self._cursor() as cursor:
            cursor.execute(q, (campaign_id,))
            r = cursor.fetchone()
        return r[0] if r else None

    def get_all_offer_names(self) -> dict:
        """Return dict of {offer_id: offer_name} for in-memory lookups."""
        q = "SELECT offer_id, offer_name FROM public.voluum_offers;"
        with self._cursor() as cursor:
            cursor.execute(q)
            rows = cursor.fetchall()
        return {r[0]: r[1] for r in rows}

    def get_all_campaign_names(self) -> dict:
        """Return dict of {campaign_id: campaign_name} for in-memory lookups."""
        q = "SELECT campaign_id, campaign_name FROM public.voluum_campaigns;"
        with self._cursor() as cursor:
            cursor.execute(q)
            rows = cursor.fetchall()
        return {r[0]: r[1] for r in rows}

    # ============================================================
    # Voluum Live Events (SQS)
    # ============================================================

    def insert_live_events(self, events: list[dict], offer_map: dict, campaign_map: dict):
        """
        Bulk upsert live events into voluum_live_events.
        Resolves offer_name and campaign_name from the provided lookup maps.
        On duplicate custom_variable_1, updates the row with the latest data.
        Returns count of rows processed.
        """
        if not events:
            return 0

        q = """
            INSERT INTO public.voluum_live_events (
                click_id, campaign_id, campaign_name, offer_id, offer_name,
                timestamp, external_id, traffic_source_id, lander_id, affiliate_network_id,
                brand, browser, browser_version, device, model,
                os, os_version, city, region, country_code,
                isp, ip, referrer, url, user_agent,
                language, mobile_carrier, connection_type, flow_id, path_id,
                custom_variable_1, custom_variable_2, custom_variable_3, custom_variable_4, custom_variable_5,
                custom_variable_6, custom_variable_7, custom_variable_8, custom_variable_9, custom_variable_10,
                updated_at
            )
            VALUES %s
            ON CONFLICT (custom_variable_1) DO UPDATE SET
                click_id = EXCLUDED.click_id,
                campaign_id = EXCLUDED.campaign_id,
                campaign_name = EXCLUDED.campaign_name,
                offer_id = EXCLUDED.offer_id,
                offer_name = EXCLUDED.offer_name,
                timestamp = EXCLUDED.timestamp,
                external_id = EXCLUDED.external_id,
                traffic_source_id = EXCLUDED.traffic_source_id,
                lander_id = EXCLUDED.lander_id,
                affiliate_network_id = EXCLUDED.affiliate_network_id,
                brand = EXCLUDED.brand,
                browser = EXCLUDED.browser,
                browser_version = EXCLUDED.browser_version,
                device = EXCLUDED.device,
                model = EXCLUDED.model,
                os = EXCLUDED.os,
                os_version = EXCLUDED.os_version,
                city = EXCLUDED.city,
                region = EXCLUDED.region,
                country_code = EXCLUDED.country_code,
                isp = EXCLUDED.isp,
                ip = EXCLUDED.ip,
                referrer = EXCLUDED.referrer,
                url = EXCLUDED.url,
                user_agent = EXCLUDED.user_agent,
                language = EXCLUDED.language,
                mobile_carrier = EXCLUDED.mobile_carrier,
                connection_type = EXCLUDED.connection_type,
                flow_id = EXCLUDED.flow_id,
                path_id = EXCLUDED.path_id,
                custom_variable_2 = EXCLUDED.custom_variable_2,
                custom_variable_3 = EXCLUDED.custom_variable_3,
                custom_variable_4 = EXCLUDED.custom_variable_4,
                custom_variable_5 = EXCLUDED.custom_variable_5,
                custom_variable_6 = EXCLUDED.custom_variable_6,
                custom_variable_7 = EXCLUDED.custom_variable_7,
                custom_variable_8 = EXCLUDED.custom_variable_8,
                custom_variable_9 = EXCLUDED.custom_variable_9,
                custom_variable_10 = EXCLUDED.custom_variable_10,
                updated_at = EXCLUDED.updated_at;
        """

        now = datetime.now()
        rows = []
        for e in events:
            # Parse customVariables array (always 10 elements)
            cvars = e.get("customVariables", [])
            cvars = (cvars + [''] * 10)[:10]

            # Skip events with no custom_variable_1
            if not cvars[0]:
                continue

            offer_id = e.get("offerId")
            campaign_id = e.get("campaignId")
            offer_name = offer_map.get(offer_id)
            campaign_name = campaign_map.get(campaign_id)

            rows.append((
                e.get("clickId"),
                campaign_id,
                campaign_name,
                offer_id,
                offer_name,
                e.get("timestamp"),
                e.get("externalId"),
                e.get("trafficSourceId"),
                e.get("landerId"),
                e.get("affiliateNetworkId"),
                e.get("brand"),
                e.get("browser"),
                e.get("browserVersion"),
                e.get("device"),
                e.get("model"),
                e.get("os"),
                e.get("osVersion"),
                e.get("city"),
                e.get("region"),
                e.get("countryCode"),
                e.get("isp"),
                e.get("ip"),
                e.get("referrer"),
                e.get("url"),
                e.get("userAgent"),
                e.get("language"),
                e.get("mobileCarrier"),
                e.get("connectionType"),
                e.get("flowId"),
                e.get("pathId"),
                cvars[0], cvars[1], cvars[2], cvars[3], cvars[4],
                cvars[5], cvars[6], cvars[7], cvars[8], cvars[9],
                now,
            ))

        if not rows:
            return 0

        with self._cursor() as cursor:
            execute_values(cursor, q, rows, page_size=self.batch_size)
        self.connection.commit()
        print(f"Upserted {len(rows)} live events into voluum_live_events.")
        return len(rows)

    def insert_raw_live_events(self, events: list[dict]):
        """
        Bulk insert raw live events into raw_live_voluum_sns_data.
        Stores everything as-is with customVariables as a TEXT[] array.
        No deduplication — every event is stored.
        """
        if not events:
            return 0

        q = """
            INSERT INTO public.raw_live_voluum_sns_data (
                click_id, campaign_id, offer_id,
                timestamp, external_id, traffic_source_id, lander_id, affiliate_network_id,
                brand, browser, browser_version, device, model,
                os, os_version, city, region, country_code,
                isp, ip, referrer, custom_variables, url, user_agent,
                language, mobile_carrier, connection_type, flow_id, path_id
            )
            VALUES %s;
        """

        rows = []
        for e in events:
            cvars = e.get("customVariables", [])

            rows.append((
                e.get("clickId"),
                e.get("campaignId"),
                e.get("offerId"),
                e.get("timestamp"),
                e.get("externalId"),
                e.get("trafficSourceId"),
                e.get("landerId"),
                e.get("affiliateNetworkId"),
                e.get("brand"),
                e.get("browser"),
                e.get("browserVersion"),
                e.get("device"),
                e.get("model"),
                e.get("os"),
                e.get("osVersion"),
                e.get("city"),
                e.get("region"),
                e.get("countryCode"),
                e.get("isp"),
                e.get("ip"),
                e.get("referrer"),
                cvars,
                e.get("url"),
                e.get("userAgent"),
                e.get("language"),
                e.get("mobileCarrier"),
                e.get("connectionType"),
                e.get("flowId"),
                e.get("pathId"),
            ))

        if not rows:
            return 0

        with self._cursor() as cursor:
            execute_values(cursor, q, rows, page_size=self.batch_size)
        self.connection.commit()
        print(f"Inserted {len(rows)} raw events into raw_live_voluum_sns_data.")
        return len(rows)
