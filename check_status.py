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
        FROM etl_log 
        WHERE STATUS = 'LOAD_DWH_SUCCESS' 
          AND DATE(RUN_DATE) = %s
    """
    
    cursor.execute(sql_check, (today,))
    result = cursor.fetchone()
    
    if result and result[0] > 0:
        print(f"ETL ngày {today} đã chạy thành công. Không cần chạy lại.")
        sys.exit(0) 
    else:
        print(f"ETL ngày {today} chưa hoàn tất. Bắt đầu chạy...")
        sys.exit(1) 
        
except Exception as e:
    print(f"Lỗi khi kiểm tra status: {e}. Coi như CHƯA CHẠY.")
    sys.exit(1) 
finally:
    if conn:
        conn.close()