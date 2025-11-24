import mysql.connector
import configparser
from datetime import datetime
import sys

# ===================== KẾT NỐI DATABASE =====================
def get_connection(config_section):
    return mysql.connector.connect(
        host=config_section['host'],
        user=config_section['user'],
        password=config_section['password'],
        database=config_section['database']
    )


# ===================== LẤY ID PROCESS =====================
def get_process_id(conn, process_name):
    cursor = conn.cursor()
    cursor.execute("SELECT ID FROM PROCESS WHERE TEN_PROCESS = %s", (process_name,))
    row = cursor.fetchone()
    cursor.close()

    if row:
        return row[0]
    else:
        raise Exception(f"PROCESS '{process_name}' không tồn tại trong bảng PROCESS.")

# ===================== KIỂM TRA TIẾN TRÌNH ĐANG CHẠY =====================
def is_process_running(conn, process_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM PROCESS_LOG
        WHERE ID_PROCESS = %s AND STATUS = 'RUNNING'
    """, (process_id,))
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

# ===================== KIỂM TRA CRAWL_LOG =====================
def is_crawl_running(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM crawl_log
        WHERE STATUS = 'RUNNING'
    """)
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

# ===================== TẠO PROCESS_LOG =====================
def create_process_log(conn, id_process):
    cursor = conn.cursor()
    sql = """
        INSERT INTO PROCESS_LOG (ID_PROCESS, START_TIME, STATUS)
        VALUES (%s, NOW(), 'RUNNING')
    """
    cursor.execute(sql, (id_process,))
    conn.commit()
    log_id = cursor.lastrowid
    cursor.close()
    return log_id

# ===================== UPDATE PROCESS_LOG =====================
def update_process_log(conn, process_log_id, status, message):
    cursor = conn.cursor()
    sql = """
        UPDATE PROCESS_LOG
        SET END_TIME = NOW(),
            STATUS = %s,
            MESSAGE = %s
        WHERE ID = %s
    """
    cursor.execute(sql, (status, message, process_log_id))
    conn.commit()
    cursor.close()

# ===================== KIỂM TRA AGGREGATE HÔM NAY =====================
def is_aggregate_loaded_today(conn_warehouse, date_sk):
    cursor = conn_warehouse.cursor()
    cursor.execute("SELECT COUNT(*) FROM AGGREGATE WHERE DATE_SK = %s", (date_sk,))
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

# ===================== LOAD AGGREGATE (ĐÃ SỬA THÊM ID_CONFIG) =====================
def load_aggregate(conn_warehouse, DATE_SK, CALENDAR_YEAR, CALENDAR_MONTH):
    cursor = conn_warehouse.cursor()
    sql = """
        INSERT INTO AGGREGATE
            (BRAND_SK, PRODUCT_SK, ID_CONFIG, DATE_SK, CALENDAR_YEAR, CALENDAR_MONTH, MAX_PRICE, MIN_PRICE)
        SELECT 
            BRAND_SK,
            PRODUCT_SK,
            ID_CONFIG,
            %s,
            %s,
            %s,
            MAX_PRICE,
            MIN_PRICE
        FROM DIM_PRODUCT
        ON DUPLICATE KEY UPDATE
            ID_CONFIG = VALUES(ID_CONFIG),
            MAX_PRICE = VALUES(MAX_PRICE),
            MIN_PRICE = VALUES(MIN_PRICE);
    """
    cursor.execute(sql, (DATE_SK, CALENDAR_YEAR, CALENDAR_MONTH))
    rowcount = cursor.rowcount
    conn_warehouse.commit()
    cursor.close()
    return rowcount

# ===================== MAIN =====================

def main():
    try:
        # Đọc config
        config = configparser.ConfigParser()

        import os
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
        print("Config path:", config_path)
        print("File exists:", os.path.exists(config_path))

        config.read(config_path)
        print("Sections:", config.sections())

        conn_control = get_connection(config["database_control"])
        conn_warehouse = get_connection(config["database_warehouse"])

        PROCESS_NAME = "LOAD_AGGREGATE"

        # Lấy ID_PROCESS
        id_process = get_process_id(conn_control, PROCESS_NAME)

        # Kiểm tra crawl_log
        if is_crawl_running(conn_control):
            print(">>> Có tiến trình crawl đang chạy. Dừng lại...")
            return

        # Kiểm tra tiến trình đang chạy
        if is_process_running(conn_control, id_process):
            print(f">>> Có tiến trình {PROCESS_NAME} đang chạy. Dừng lại...")
            return

        # Lấy DATE_SK hôm nay
        cursor = conn_warehouse.cursor()
        cursor.execute("SELECT DATE_SK, CALENDAR_YEAR, INT_MONTH FROM DIM_DATE WHERE FULL_DATE = CURDATE()")
        row = cursor.fetchone()
        cursor.close()

        if not row:
            raise Exception("DIM_DATE chưa có CURDATE().")

        DATE_SK, CALENDAR_YEAR, CALENDAR_MONTH = row

        # Kiểm tra load hôm nay
        if is_aggregate_loaded_today(conn_warehouse, DATE_SK):
            print(f">>> AGGREGATE đã được load hôm nay (DATE_SK={DATE_SK}). Dừng lại...")
            return

        # Log START
        process_log_id = create_process_log(conn_control, id_process)
        print(f">>> Bắt đầu LOAD_AGGREGATE. Log ID = {process_log_id}")

        # Thực thi ETL
        rows = load_aggregate(conn_warehouse, DATE_SK, CALENDAR_YEAR, CALENDAR_MONTH)

        # Log SUCCESS
        update_process_log(conn_control, process_log_id, "SUCCESS",
                           f"Nạp thành công {rows} dòng vào bảng AGGREGATE.")
        print(f">>> Thành công! Đã nạp {rows} dòng vào AGGREGATE.")
        
    except Exception as e:
        print("!!! Lỗi:", str(e))
        try:
            if "process_log_id" in locals():
                update_process_log(conn_control, process_log_id, "FAIL", str(e))
        except:
            pass

    finally:
        try:
            if conn_control and conn_control.is_connected():
                conn_control.close()
            if conn_warehouse and conn_warehouse.is_connected():
                conn_warehouse.close()
            print(">>> Đã đóng toàn bộ kết nối.")
        except:
            pass


if __name__ == "__main__":
    main()
