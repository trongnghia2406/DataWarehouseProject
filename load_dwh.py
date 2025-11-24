import mysql.connector
import configparser
import os
from datetime import datetime

# ===================== Config =====================
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

DB_CONTROL_CONFIG = config['database_control']
DB_STAGING_CONFIG = config['database_staging']
DB_WAREHOUSE_CONFIG = config['database_warehouse']

PROCESS_NAME = 'Load_DWH'

# ===================== Helper functions =====================
def get_or_create_sk(cursor, table_name, lookup_fields, data_fields=None, sk_name=None):
    if sk_name is None:
        sk_name = f"{table_name.replace('DIM_', '')}_SK"

    where_clauses = []
    params = []
    for key, value in lookup_fields.items():
        if value is None:
            where_clauses.append(f"`{key}` IS NULL")
        else:
            where_clauses.append(f"`{key}` = %s")
            params.append(value)

    sql_select = f"SELECT {sk_name} FROM {table_name} WHERE {' AND '.join(where_clauses)} LIMIT 1"
    cursor.execute(sql_select, tuple(params))
    result = cursor.fetchone()
    if result:
        return result[sk_name] if isinstance(result, dict) else result[0]

    if data_fields is None:
        data_fields = lookup_fields

    columns = ', '.join(f"`{k}`" for k in data_fields.keys())
    placeholders = ', '.join(['%s'] * len(data_fields))
    sql_insert = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(sql_insert, tuple(data_fields.values()))
    return cursor.lastrowid

def get_process_id(conn, process_name):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT ID FROM PROCESS WHERE TEN_PROCESS=%s AND IS_ACTIVE=TRUE", (process_name,))
    row = cursor.fetchone()
    cursor.close()
    if row:
        return row['ID']
    else:
        raise Exception(f"PROCESS '{process_name}' không tồn tại trong bảng PROCESS.")

def any_process_running(conn, process_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM PROCESS_LOG
        WHERE ID_PROCESS = %s AND END_TIME IS NULL
    """, (process_id,))
    running_count = cursor.fetchone()[0]
    cursor.close()
    return running_count > 0

def is_crawl_running(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM crawl_log
        WHERE STATUS='RUNNING'
    """)
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

def insert_process_log(conn, process_id):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO PROCESS_LOG (ID_PROCESS, STATUS, START_TIME) VALUES (%s, %s, %s)",
        (process_id, 'RUNNING', datetime.now())
    )
    conn.commit()
    log_id = cursor.lastrowid
    cursor.close()
    return log_id

def update_process_log(conn, log_id, status, message=''):
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE PROCESS_LOG SET END_TIME=%s, STATUS=%s, MESSAGE=%s WHERE ID=%s",
        (datetime.now(), status, message, log_id)
    )
    conn.commit()
    cursor.close()

# ===================== MAIN =====================
def main():
    conn_control = conn_staging = conn_warehouse = None
    try:
        # ===== Connect to databases =====
        conn_control = mysql.connector.connect(**DB_CONTROL_CONFIG)
        conn_staging = mysql.connector.connect(**DB_STAGING_CONFIG)
        conn_warehouse = mysql.connector.connect(**DB_WAREHOUSE_CONFIG)

        process_id = get_process_id(conn_control, PROCESS_NAME)

        if is_crawl_running(conn_control):
            print(">>> Có tiến trình crawl đang chạy. Dừng load DIM.")
            return

        if any_process_running(conn_control, process_id):
            print(">>> Có tiến trình Load_DWH đang chạy. Dừng load DIM.")
            return

        process_log_id = insert_process_log(conn_control, process_id)
        print(f">>> Bắt đầu Load_DWH. Log ID = {process_log_id}")

        # ===== Lấy DATE_SK hiện tại =====
        cursor_dw = conn_warehouse.cursor(dictionary=True)
        cursor_dw.execute("SELECT DATE_SK FROM DIM_DATE WHERE FULL_DATE = CURDATE()")
        date_row = cursor_dw.fetchone()
        if not date_row:
            raise Exception("Không tìm thấy CURDATE() trong DIM_DATE.")
        CURRENT_DATE_SK = date_row['DATE_SK']
        cursor_dw.close()

        # ===== Lấy dữ liệu từ staging =====
        cursor_stg = conn_staging.cursor(dictionary=True)
        cursor_stg.execute("""
            SELECT *
            FROM PRODUCTS_EXPIRED
            WHERE EXPIRED_AT = '9999-12-31'
        """)
        rows = cursor_stg.fetchall()
        cursor_dw_sk = conn_warehouse.cursor(dictionary=True)

        count_loaded = 0

        for r in rows:
            # ===== Xác định BRAND =====
            brand_name = "Khác"
            if r['TEN']:
                t = r['TEN'].lower()
                if "iphone" in t: brand_name = "Apple"
                elif "samsung" in t: brand_name = "Samsung"
                elif "xiaomi" in t: brand_name = "Xiaomi"
                elif "oppo" in t: brand_name = "OPPO"
                elif "vivo" in t: brand_name = "Vivo"
                elif "realme" in t: brand_name = "Realme"
                elif "nokia" in t: brand_name = "Nokia"
                elif "asus" in t: brand_name = "ASUS"

            brand_sk = get_or_create_sk(cursor_dw_sk, 'DIM_BRAND',
                                        {'BRAND_NAME': brand_name}, sk_name='BRAND_SK')

            # ===== Kiểm tra sản phẩm đã có trong DIM_PRODUCT chưa =====
            cursor_dw_sk.execute("""
                SELECT PRODUCT_SK, MIN_PRICE, MAX_PRICE
                FROM DIM_PRODUCT
                WHERE LINK=%s
            """, (r['LINK'],))
            product_row = cursor_dw_sk.fetchone()

            # ===== Tính MIN_PRICE và MAX_PRICE =====
            gia_cu = r.get('GIA_CU')
            gia_moi = r.get('GIA_MOI')

            if (gia_cu is None or gia_cu == -1) and (gia_moi is not None and gia_moi != -1):
                new_min = gia_moi
                new_max = gia_moi
            elif gia_cu is not None and gia_cu != -1 and (gia_moi is not None and gia_moi != -1):
                new_min = min(gia_cu, gia_moi)
                new_max = max(gia_cu, gia_moi)
            elif gia_cu is not None and gia_cu != -1:
                new_min = gia_cu
                new_max = gia_cu
            elif gia_moi is not None and gia_moi != -1:
                new_min = gia_moi
                new_max = gia_moi
            else:
                new_min = None
                new_max = None

            # ===== UPDATE DIM_PRODUCT =====
            if product_row:
                product_sk = product_row['PRODUCT_SK']
                cursor_dw_sk.execute("""
                    UPDATE DIM_PRODUCT
                    SET TEN=%s,
                        LINK_ANH=%s,
                        KICH_THUOC_MAN_HINH=%s,
                        RAM=%s,
                        BO_NHO=%s,
                        BRAND_SK=%s,
                        MIN_PRICE=%s,
                        MAX_PRICE=%s,
                        ID_CONFIG=%s
                    WHERE PRODUCT_SK=%s
                """, (
                    r['TEN'], r['LINK_ANH'], r['KICH_THUOC_MAN_HINH'],
                    r['RAM'], r['BO_NHO'], brand_sk,
                    new_min, new_max,
                    r['ID_CONFIG'],
                    product_sk
                ))
            else:
                # ===== INSERT DIM_PRODUCT =====
                get_or_create_sk(
                    cursor_dw_sk, 'DIM_PRODUCT', {'LINK': r['LINK']},
                    {
                        'TEN': r['TEN'],
                        'LINK': r['LINK'],
                        'LINK_ANH': r['LINK_ANH'],
                        'KICH_THUOC_MAN_HINH': r['KICH_THUOC_MAN_HINH'],
                        'RAM': r['RAM'],
                        'BO_NHO': r['BO_NHO'],
                        'MIN_PRICE': new_min,
                        'MAX_PRICE': new_max,
                        'BRAND_SK': brand_sk,
                        'ID_CONFIG': r['ID_CONFIG']
                    },
                    sk_name='PRODUCT_SK'
                )

            count_loaded += 1

        conn_warehouse.commit()
        cursor_stg.close()
        cursor_dw_sk.close()

        update_process_log(conn_control, process_log_id, "SUCCESS",
                           f"Đã load {count_loaded} sản phẩm từ PRODUCTS_EXPIRED")
        print(f">>> Thành công! Đã load {count_loaded} sản phẩm.")

    except Exception as e:
        import traceback
        print("!!! Lỗi:", repr(e))
        traceback.print_exc()
        if conn_control and 'process_log_id' in locals():
            update_process_log(conn_control, process_log_id, "FAILED", repr(e))

    finally:
        if conn_control: conn_control.close()
        if conn_staging: conn_staging.close()
        if conn_warehouse: conn_warehouse.close()
        print(">>> Đã đóng toàn bộ kết nối.")
        print("Config path:", config_path)
        print("File exists:", os.path.exists(config_path))
        print("Sections:", config.sections())

if __name__ == "__main__":
    main()
