import mysql.connector
import csv
import os
import configparser
import datetime
import glob
import json

# =======================
# ĐỌC CONFIG
# =======================
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

DB_CONTROL = config['database_control']
DB_STAGING = config['database_staging']

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


# =======================
# TÌM FILE CSV MỚI NHẤT
# =======================
def find_latest_csv():
    try:
        files = glob.glob(os.path.join(CURRENT_DIR, "products_raw_*.csv"))
        if not files:
            return None
        return max(files, key=os.path.getmtime)
    except:
        return None


# =======================
# GHI LOG
# =======================
def log_message(conn, status, rows=None, details=None, process_id=None):
    try:
        cursor = conn.cursor()

        msg_json = json.dumps({
            "rows": rows,
            "details": details
        }, ensure_ascii=False)

        cursor.execute("""
            INSERT INTO process_log (ID_PROCESS, STATUS, MESSAGE)
            VALUES (%s, %s, %s)
        """, (process_id, status, msg_json))

        conn.commit()
        cursor.close()

    except Exception as e:
        print("Lỗi khi ghi PROCESS_LOG:", e)


# =======================
# LẤY CẤU TRÚC BẢNG
# =======================
def get_db_columns(cursor):
    cursor.execute("DESCRIBE PRODUCTS_GENERAL")
    rows = cursor.fetchall()
    return [r[0] for r in rows]


# =======================
# LẤY ID_CONFIG TỪ CONFIG
# =======================
def get_id_config(conn_control, site_name):
    cursor = conn_control.cursor(dictionary=True)

    cursor.execute("""
        SELECT ID 
        FROM config
        WHERE TEN = %s
        LIMIT 1
    """, (site_name,))

    result = cursor.fetchone()
    cursor.close()
    return result["ID"] if result else None


# =======================
# LẤY RUN_DATE TỪ CRAWL_LOG THEO ID_CONFIG
# =======================
def get_latest_run_date(conn_control, id_config):
    cursor = conn_control.cursor(dictionary=True)

    cursor.execute("""
        SELECT RUN_DATE
        FROM crawl_log
        WHERE ID_CONFIG = %s
        ORDER BY RUN_DATE DESC
        LIMIT 1
    """, (id_config,))

    result = cursor.fetchone()
    cursor.close()
    return result["RUN_DATE"] if result else None


# =======================
# CHƯƠNG TRÌNH CHÍNH
# =======================
def main():
    print("\n--- BẮT ĐẦU LOAD STAGING (CSV → DB) ---")

    conn_c = None
    conn_s = None

    try:
        conn_c = mysql.connector.connect(**DB_CONTROL)
        conn_s = mysql.connector.connect(**DB_STAGING)
        cursor_s = conn_s.cursor()

        # Tìm file CSV
        csv_file = find_latest_csv()
        if not csv_file:
            print("!!! Không tìm thấy file CSV.")
            log_message(conn_c, "LOAD_STAGING_FAIL", 0, {"error": "Không tìm thấy CSV"})
            return

        print(f">>>>> Đang đọc: {csv_file}")

        # Đọc CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            csv_data = list(reader)

        if not csv_data:
            print("!!! CSV rỗng.")
            log_message(conn_c, "LOAD_STAGING_WARN", 0, {"error": "CSV rỗng"})
            return

        print(f">>>>> {len(csv_data)} dòng CSV")

        # Lấy danh sách cột DB
        db_columns = get_db_columns(cursor_s)

        # Xác định cột hợp lệ
        insert_columns = [col for col in db_columns if col in csv_data[0].keys()]

        # Thêm 2 cột system
        if "NGAY" in db_columns:
            insert_columns.append("NGAY")
        if "ID_CONFIG" in db_columns:
            insert_columns.append("ID_CONFIG")

        print(f">>>>> Cột ghi vào DB: {insert_columns}")

        # Câu SQL Insert
        col_str = ", ".join(insert_columns)
        val_str = ", ".join(["%s"] * len(insert_columns))
        sql_insert = f"INSERT INTO PRODUCTS_GENERAL ({col_str}) VALUES ({val_str})"

        # Xóa dữ liệu cũ
        cursor_s.execute("TRUNCATE TABLE PRODUCTS_GENERAL")
        print(">>>>>> Xóa dữ liệu cũ xong.")

        # Chuẩn bị data insert từng dòng
        tuples = []

        for row in csv_data:
            site_name = row.get("SITE_NAME")

            # 1) Lấy ID_CONFIG riêng cho từng dòng
            id_config = get_id_config(conn_c, site_name)

            # 2) Lấy RUN_DATE riêng cho từng site
            run_date = get_latest_run_date(conn_c, id_config)

            # Gán dữ liệu
            values = [row.get(col) for col in insert_columns]

            if "NGAY" in insert_columns:
                values[insert_columns.index("NGAY")] = run_date

            if "ID_CONFIG" in insert_columns:
                values[insert_columns.index("ID_CONFIG")] = id_config

            tuples.append(tuple(values))

        # Insert batch
        cursor_s.executemany(sql_insert, tuples)
        conn_s.commit()

        print(f">>>>> ĐÃ INSERT {cursor_s.rowcount} ROWS.")
        log_message(conn_c, "LOAD_STAGING_SUCCESS", cursor_s.rowcount,
                    {"source": os.path.basename(csv_file), "insert_cols": insert_columns})

    except Exception as e:
        if conn_s:
            conn_s.rollback()

        print("LỖI:", e)
        log_message(conn_c, "LOAD_STAGING_FAIL", 0, {"error": str(e)})

    finally:
        if conn_s:
            conn_s.close()
        if conn_c:
            conn_c.close()

        print(">>> ĐÃ ĐÓNG KẾT NỐI.")


if __name__ == "__main__":
    main()
