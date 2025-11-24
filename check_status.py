import mysql.connector
import configparser
import os
import datetime
import sys

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)
DB_CONTROL_CONFIG = config['database_control']

conn = None
try:
    conn = mysql.connector.connect(**DB_CONTROL_CONFIG)
    cursor = conn.cursor()
    
    today = datetime.date.today()
    
    sql_check = """
        SELECT COUNT(*) 
        FROM PROCESS_LOG pl
        JOIN PROCESS p ON pl.ID_PROCESS = p.ID
        WHERE p.TEN_PROCESS = 'Load_DataMart' 
          AND pl.STATUS = 'SUCCESS' 
          AND DATE(pl.END_TIME) = %s
    """
    
    cursor.execute(sql_check, (today,))
    result = cursor.fetchone()
    
    if result and result[0] > 0:
        print(f"Hệ thống ngày {today} đã hoàn tất (Load_DataMart SUCCESS). Không chạy lại.")
        sys.exit(0) 
    else:
        print(f"Hệ thống ngày {today} chưa hoàn tất. Bắt đầu chạy...")
        sys.exit(1) 
        
except Exception as e:
    print(f"Lỗi kiểm tra status: {e}. Coi như CHƯA CHẠY.")
    sys.exit(1) 
finally:
    if conn: conn.close()