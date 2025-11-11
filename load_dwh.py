import mysql.connector
import configparser
import os
import json

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

DB_CONTROL_CONFIG = config['database_control']
DB_STAGING_CONFIG = config['database_staging']
DB_WAREHOUSE_CONFIG = config['database_warehouse']

def log_message(conn, config_id, status, rows=None, details_dict=None):
    """Ghi log có cấu trúc vào etl_log."""
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

def get_or_create_sk(cursor, table_name, lookup_fields, data_fields=None, sk_name=None):
    """
    Hàm tra cứu hoặc tạo mới Surrogate Key cho các bảng DIM.
    Ví dụ: get_or_create_sk(cursor, 'DIM_BRAND', {'BRAND_NAME': 'Apple'}, sk_name='BRAND_SK')
    """
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
    
    if not where_clauses:
        raise ValueError(f"Không có trường tra cứu (lookup_fields) cho bảng {table_name}")

    sql_select = f"SELECT {sk_name} FROM {table_name} WHERE {' AND '.join(where_clauses)} LIMIT 1"
    cursor.execute(sql_select, tuple(params))
    result = cursor.fetchone()
    
    if result:
        return result[0] 

    if data_fields is None:
        data_fields = lookup_fields
        
    columns = []
    values = []
    for key, value in data_fields.items():
        columns.append(f"`{key}`")
        values.append(value)
    
    sql_insert = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"
    cursor.execute(sql_insert, tuple(values))
    
    return cursor.lastrowid 
def main():
    print("--- Bắt đầu Bước 4: Load Data Warehouse (DIMs & FACT) ---")
    conn_control = None
    conn_staging = None
    conn_warehouse = None
    
    try:
        conn_control = mysql.connector.connect(**DB_CONTROL_CONFIG)
        conn_staging = mysql.connector.connect(**DB_STAGING_CONFIG)
        conn_warehouse = mysql.connector.connect(**DB_WAREHOUSE_CONFIG)
        
        cursor_read = conn_staging.cursor(dictionary=True) 
        cursor_write_dict = conn_warehouse.cursor(dictionary=True)
        cursor_write_sk = conn_warehouse.cursor() 

        print(">>>>>>>>> Đã kết nối 3 DB thành công.")

        cursor_write_dict.execute("SELECT DATE_SK FROM DIM_DATE WHERE FULL_DATE = CURDATE()")
        date_row = cursor_write_dict.fetchone()
        if not date_row:
            print("!!! LỖI: Không tìm thấy ngày hôm nay trong DIM_DATE. Hãy chạy script 'populate_dim_date.py'.")
            log_message(conn_control, None, "LOAD_DWH_FAIL", rows=0, details_dict={"error": "Không tìm thấy CURDATE() trong DIM_DATE"})
            return
        
        CURRENT_DATE_SK = date_row['DATE_SK']
        print(f">>>>>>>>> Đã lấy DATE_SK cho hôm nay: {CURRENT_DATE_SK}")

        cursor_write_sk.execute("DELETE FROM FACT_PRODUCT_SALES WHERE DATE_SK = %s", (CURRENT_DATE_SK,))
        deleted_count = cursor_write_sk.rowcount
        conn_warehouse.commit() 
        print(f"-> Đã xóa {deleted_count} bản ghi fact cũ của ngày hôm nay.")

        cursor_read.execute("SELECT * FROM PRODUCTS_TRANSFORM WHERE EXPIRED_AT = '9999-12-31'")
        source_rows = cursor_read.fetchall()
        print(f">>>>>>>>> Đã đọc {len(source_rows)} bản ghi active từ 'PRODUCTS_TRANSFORM'.")

        count_fact_inserted = 0
        count_error = 0
        
        for row in source_rows:
            try:
                brand_name = "Khác" 
                if row['TEN']:
                    if "iPhone" in row['TEN']: brand_name = "Apple"
                    elif "Samsung" in row['TEN'] or "Galaxy" in row['TEN']: brand_name = "Samsung"
                    elif "Xiaomi" in row['TEN'] or "Redmi" in row['TEN']: brand_name = "Xiaomi"
                    elif "OPPO" in row['TEN']: brand_name = "OPPO"
                    elif "Vivo" in row['TEN']: brand_name = "Vivo"
                    elif "Realme" in row['TEN']: brand_name = "Realme"
                    elif "Nokia" in row['TEN']: brand_name = "Nokia"
                    elif "ASUS" in row['TEN']: brand_name = "ASUS"
                
                brand_sk = get_or_create_sk(
                    cursor_write_sk, 
                    'DIM_BRAND', 
                    {'BRAND_NAME': brand_name},
                    sk_name='BRAND_SK'
                )

                promo_sk = get_or_create_sk(
                    cursor_write_sk,
                    'DIM_PROMOTION',
                    { 
                        'GIAM_GIA_SMEMBER': row['GIAM_GIA_SMEMBER'],
                        'GIAM_GIA_SSTUDENT': row['GIAM_GIA_SSTUDENT'],
                        'COUPON': row['COUPON'],
                        'QUA_TANG': row['QUA_TANG']
                    },
                    sk_name='PROMOTION_SK'
                )

                product_sk = get_or_create_sk(
                    cursor_write_sk,
                    'DIM_PRODUCT',
                    {'LINK': row['LINK']}, 
                    { 
                        'TEN': row['TEN'],
                        'LINK': row['LINK'],
                        'LINK_ANH': row['LINK_ANH'],
                        'KICH_THUOC_MAN_HINH': row['KICH_THUOC_MAN_HINH'],
                        'RAM': row['RAM'],
                        'BO_NHO': row['BO_NHO']
                    },
                    sk_name='PRODUCT_SK'
                )
                sql_insert_fact = """
                    INSERT INTO FACT_PRODUCT_SALES
                    (PRODUCT_SK, DATE_SK, PROMOTION_SK, BRAND_SK, GIA_CU, GIA_MOI,
                     GIAM_GIA_PHAN_TRAM, DA_BAN, DANH_GIA)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                fact_data = (
                    product_sk,
                    CURRENT_DATE_SK,
                    promo_sk,
                    brand_sk,
                    row['GIA_CU'],
                    row['GIA_MOI'],
                    row['GIAM_GIA_PHAN_TRAM'],
                    row['DA_BAN'],
                    row['DANH_GIA']
                )
                cursor_write_sk.execute(sql_insert_fact, fact_data)
                count_fact_inserted += 1

            except Exception as e:
                print(f"Lỗi khi xử lý Fact (LINK: {row.get('LINK')}): {e}")
                log_message(conn_control, row.get('ID_CONFIG'), "LOAD_DWH_FAIL", rows=0, 
                            details_dict={"error": "Lỗi nạp Fact", "details": str(e), "product_name": row.get('TEN')})
                count_error += 1

        conn_warehouse.commit()
        log_message(conn_control, None, "LOAD_DWH_SUCCESS", rows=count_fact_inserted, 
                    details_dict={"inserted": count_fact_inserted, "errors": count_error})
        
        print(f"\n--- HOÀN TẤT ---")
        print(f">>>>>>>>> Đã nạp {count_fact_inserted} bản ghi vào FACT_PRODUCT_SALES.")

    except Exception as e:
        if conn_warehouse: conn_warehouse.rollback()
        if conn_control:
            log_message(conn_control, None, "LOAD_DWH_FAIL", rows=0, details_dict={"error": "Lỗi nghiêm trọng", "details": str(e)})
        print(f"\n!!! LỖI NGHIÊM TRỌNG KHI LOAD DWH: {e}")
        
    finally:
        if 'cursor_read' in locals() and cursor_read: cursor_read.close()
        if 'cursor_write_dict' in locals() and cursor_write_dict: cursor_write_dict.close()
        if 'cursor_write_sk' in locals() and cursor_write_sk: cursor_write_sk.close()
        if conn_staging: conn_staging.close()
        if conn_warehouse: conn_warehouse.close()
        if conn_control: conn_control.close()
        print(">>>>>>>>> Đã đóng tất cả kết nối DB.")

if __name__ == "__main__":
    main()