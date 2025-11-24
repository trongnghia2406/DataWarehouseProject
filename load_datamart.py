import mysql.connector
import configparser
import os
from datetime import datetime

# ===================== CONFIG =====================
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
config.read(config_path)

DB_CONTROL = config["database_control"]
DB_DWH = config["database_warehouse"]
DB_DM = config["database_datamart"]

PROCESS_NAME = "Load_DataMart"

DB_NAME_DWH = DB_DWH["database"]
DB_NAME_DM = DB_DM["database"]

# ================= Helper Functions =================
def log_process_start(conn, process_id):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO PROCESS_LOG (ID_PROCESS, STATUS, START_TIME) VALUES (%s, %s, %s)",
        (process_id, "RUNNING", datetime.now())
    )
    conn.commit()
    log_id = cursor.lastrowid
    cursor.close()
    return log_id

def log_process_end(conn, log_id, status, message=""):
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE PROCESS_LOG SET END_TIME=%s, STATUS=%s, MESSAGE=%s WHERE ID=%s",
        (datetime.now(), status, message, log_id)
    )
    conn.commit()
    cursor.close()

def any_process_running(conn, process_name):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pl.ID
        FROM PROCESS_LOG pl
        JOIN PROCESS p ON pl.ID_PROCESS = p.ID
        WHERE p.TEN_PROCESS=%s AND pl.END_TIME IS NULL
    """, (process_name,))
    running = cursor.fetchall()
    cursor.close()
    return bool(running)

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

def get_process_id(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT ID FROM PROCESS WHERE TEN_PROCESS=%s AND IS_ACTIVE=TRUE", (PROCESS_NAME,))
    row = cursor.fetchone()
    if not row:
        cursor2 = conn.cursor()
        cursor2.execute(
            "INSERT INTO PROCESS (TEN_PROCESS, SOURCE_TABLE, TARGET_TABLE, IS_ACTIVE) VALUES (%s,%s,%s,%s)",
            (PROCESS_NAME, "AGGREGATE", "DATAMART_TABLES", True)
        )
        conn.commit()
        new_id = cursor2.lastrowid
        cursor2.close()
        cursor.close()
        return new_id
    cursor.close()
    return row["ID"]

# ================== MAIN ==================
def main():
    conn_control = conn_dwh = conn_dm = None
    try:
        # ===== Connect to Databases =====
        conn_control = mysql.connector.connect(**DB_CONTROL)
        conn_dwh = mysql.connector.connect(**DB_DWH)
        conn_dm = mysql.connector.connect(**DB_DM)

        # ===== Check Running Crawl Process =====
        if is_crawl_running(conn_control):
            print("!!! Có tiến trình crawl đang chạy. Dừng Load DataMart.")
            return

        # ===== Check Running Load_DataMart Process =====
        if any_process_running(conn_control, PROCESS_NAME):
            print(f"!!! Tiến trình '{PROCESS_NAME}' đang chạy. Dừng Load DataMart.")
            return

        # ===== Create/Find PROCESS =====
        process_id = get_process_id(conn_control)

        # ===== Insert PROCESS_LOG (RUNNING) =====
        process_log_id = log_process_start(conn_control, process_id)
        print(f">>> Bắt đầu Load DataMart. Process Log ID = {process_log_id}")

        cursor_dm = conn_dm.cursor()

        # =========================== LOAD MONTHLY ===========================
        print(">>> Load bảng DM_PRODUCT_DAILY_PRICE ...")
        cursor_dm.execute("DELETE FROM DM_PRODUCT_DAILY_PRICE")
        conn_dm.commit()

        sql_monthly = f"""
            INSERT INTO DM_PRODUCT_DAILY_PRICE
            (DATE_SK, PRODUCT_SK, BRAND_SK, ID_CONFIG, CALENDAR_YEAR, CALENDAR_MONTH, 
             MAX_PRICE, MIN_PRICE, AVG_PRICE)
            SELECT
                DATE_SK,
                PRODUCT_SK,
                BRAND_SK,
                ID_CONFIG,
                CALENDAR_YEAR,
                CALENDAR_MONTH,
                MAX(MAX_PRICE),
                MIN(MIN_PRICE),
                (MAX(MAX_PRICE) + MIN(MIN_PRICE)) / 2
            FROM {DB_NAME_DWH}.AGGREGATE
            GROUP BY DATE_SK, PRODUCT_SK, BRAND_SK, ID_CONFIG, CALENDAR_YEAR, CALENDAR_MONTH
        """
        cursor_dm.execute(sql_monthly)
        conn_dm.commit()
        print(">>> Load MONTHLY xong")

        # =========================== LOAD QUARTERLY ===========================
        print(">>> Load bảng DM_PRODUCT_QUARTERLY_TREND ...")
        cursor_dm.execute("DELETE FROM DM_PRODUCT_QUARTERLY_TREND")
        conn_dm.commit()

        sql_quarterly = f"""
            INSERT INTO DM_PRODUCT_QUARTERLY_TREND
            (PRODUCT_SK, BRAND_SK, ID_CONFIG, CALENDAR_YEAR, QUARTER_NAME,
             QUARTER_MIN_PRICE, QUARTER_MAX_PRICE, QUARTER_AVG_PRICE)
            SELECT 
                a.PRODUCT_SK,
                a.BRAND_SK,
                a.ID_CONFIG,
                a.CALENDAR_YEAR,
                CONCAT('Q', d.QUARTER),
                MIN(a.MIN_PRICE),
                MAX(a.MAX_PRICE),
                (MIN(a.MIN_PRICE) + MAX(a.MAX_PRICE)) / 2
            FROM {DB_NAME_DWH}.AGGREGATE a
            JOIN {DB_NAME_DWH}.DIM_DATE d ON a.DATE_SK = d.DATE_SK
            GROUP BY 
                a.PRODUCT_SK, a.BRAND_SK, a.ID_CONFIG, a.CALENDAR_YEAR, d.QUARTER
        """
        cursor_dm.execute(sql_quarterly)
        conn_dm.commit()
        print(">>> Load QUARTERLY xong")

        # ===== SUCCESS =====
        log_process_end(conn_control, process_log_id, "SUCCESS", "Load DataMart thành công")
        print(">>> Hoàn tất Load DataMart.")

    except Exception as e:
        print("!!! Lỗi:", e)
        if conn_control and "process_log_id" in locals():
            log_process_end(conn_control, process_log_id, "FAIL", str(e))

    finally:
        if conn_control: conn_control.close()
        if conn_dwh: conn_dwh.close()
        if conn_dm: conn_dm: conn_dm.close()
        print(">>> Đóng toàn bộ kết nối.")


if __name__ == "__main__":
    main()
