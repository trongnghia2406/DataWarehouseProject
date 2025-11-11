import mysql.connector
import csv
import os
import configparser
import datetime
import glob 
import json 

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

DB_CONTROL_CONFIG = config['database_control']
DB_STAGING_CONFIG = config['database_staging']

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def find_latest_csv_file():
    """Tìm file products_raw_*.csv mới nhất trong thư mục hiện tại."""
    try:
        list_of_files = glob.glob(os.path.join(CURRENT_DIR, 'products_raw_*.csv')) 
        if not list_of_files:
            return None 
        
        latest_file = max(list_of_files, key=os.path.getmtime)
        return latest_file
    except Exception as e:
        print(f"Lỗi khi tìm file CSV: {e}")
        return None

def log_message(conn, config_id, status, rows=None, details_dict=None):
    """
    Ghi log có cấu trúc vào etl_log.
    details_dict sẽ được chuyển thành chuỗi JSON.
    """
    try:
        log_cursor = conn.cursor()
        
        message_json = None
        if details_dict:
            message_json = json.dumps(details_dict, ensure_ascii=False) 
        
        sql = "INSERT INTO etl_log (ID_CONFIG, STATUS, MESSAGE, ROWS_AFFECTED) VALUES (%s, %s, %s, %s)"
        log_cursor.execute(sql, (config_id, status, message_json, rows))
        
        conn.commit()
        log_cursor.close()
    except Exception as e:
        print(f"Lỗi khi ghi log: {e}")

def main():
    print("--- Bắt đầu Bước 2: Load Staging (CSV -> DB) ---")
    conn_control = None
    conn_staging = None
    
    try:
        conn_control = mysql.connector.connect(**DB_CONTROL_CONFIG)
        conn_staging = mysql.connector.connect(**DB_STAGING_CONFIG)
        cursor_staging = conn_staging.cursor()

        csv_file_to_load = find_latest_csv_file()
        
        if not csv_file_to_load:
            print(f"!!! LỖI: Không tìm thấy file CSV (products_raw_*.csv) nào. Bạn đã chạy 'crawl.py' chưa?")
            log_message(conn_control, None, "LOAD_STAGING_FAIL", rows=0, details_dict={"error": "Không tìm thấy file CSV nguồn"})
            return

        print(f">>>>>>>>> Đang đọc file: '{csv_file_to_load}'")
        
        with open(csv_file_to_load, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            data_to_insert = [row for row in reader]
        
        if not data_to_insert:
            print(">>>>>>>>> File CSV rỗng, không có gì để load.")
            log_message(conn_control, None, "LOAD_STAGING_WARN", rows=0, details_dict={"error": "File CSV rỗng", "file_path": csv_file_to_load})
            return

        print(f">>>>>>>>> Đã đọc {len(data_to_insert)} dòng từ file CSV.")
        
        cursor_staging.execute("TRUNCATE TABLE PRODUCTS_GENERAL")
        print(">>>>>>>>> Đã xóa dữ liệu cũ trong 'PRODUCTS_GENERAL'.")

        sql_insert = """
            INSERT INTO PRODUCTS_GENERAL
            (ID_CONFIG, TEN, LINK, LINK_ANH, GIA_CU, GIA_MOI, KICH_THUOC_MAN_HINH,
             RAM, BO_NHO, GIAM_GIA_SMEMBER, GIAM_GIA_SSTUDENT, GIAM_GIA_PHAN_TRAM,
             COUPON, QUA_TANG, DANH_GIA, DA_BAN)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """
        
        tuples_to_insert = [
            (
                row.get('SITE_ID'), 
                row.get('TEN'), row.get('LINK'), row.get('LINK_ANH'), row.get('GIA_CU'),
                row.get('GIA_MOI'), row.get('KICH_THUOC_MAN_HINH'), row.get('RAM'), row.get('BO_NHO'),
                row.get('GIAM_GIA_SMEMBER'), row.get('GIAM_GIA_SSTUDENT'),
                row.get('GIAM_GIA_PHAN_TRAM'), row.get('COUPON'), row.get('QUA_TANG'),
                row.get('DANH_GIA'), row.get('DA_BAN')
            ) for row in data_to_insert
        ]

        cursor_staging.executemany(sql_insert, tuples_to_insert)
        
        count = cursor_staging.rowcount 
        
        log_message(conn_control, None, "LOAD_STAGING_SUCCESS", rows=count, 
                    details_dict={"source_file": os.path.basename(csv_file_to_load), "target_table": "PRODUCTS_GENERAL"})
        
        conn_staging.commit() 
        
        print(f"\n--- HOÀN TẤT ---")
        print(f">>>>>>>>> Đã load {count} sản phẩm vào 'PRODUCTS_GENERAL'.")

    except FileNotFoundError: 
        print(f"!!! LỖI: Không tìm thấy file {csv_file_to_load}.")
        if conn_control: log_message(conn_control, None, "LOAD_STAGING_FAIL", rows=0, details_dict={"error": f"Không tìm thấy file {csv_file_to_load}"})
    except Exception as e:
        if conn_staging: conn_staging.rollback() 
        if conn_control: log_message(conn_control, None, "LOAD_STAGING_FAIL", rows=0, details_dict={"error": "Lỗi nghiêm trọng", "details": str(e)})
        print(f"Lỗi nghiêm trọng khi load staging: {e}")
    finally:
        if 'cursor_staging' in locals() and cursor_staging: cursor_staging.close()
        if conn_staging: conn_staging.close()
        if conn_control: conn_control.close()
        print(">>>>>>>>> Đã đóng kết nối DB.")

if __name__ == "__main__":
    main()