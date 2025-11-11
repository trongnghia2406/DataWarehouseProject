import mysql.connector 
import re 
import configparser
import os
import datetime
import json

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

DB_CONTROL_CONFIG = config['database_control']
DB_STAGING_CONFIG = config['database_staging']

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

def clean_price(price_str):
    if not price_str: return None
    cleaned = re.sub(r'[^\d]', '', price_str)
    try: return float(cleaned)
    except ValueError: return None
def clean_spec(spec_str, unit):
    if not spec_str: return None
    match = re.search(r'(\d+[\.,]?\d*)', spec_str.replace(',', '.'))
    if match:
        try:
            val = float(match.group(1))
            if 'T' in spec_str.upper() and unit.upper() == 'GB': return val * 1024
            return val
        except ValueError: return None
    return None
def clean_rating(rating_str):
    if not rating_str: return None
    match = re.search(r'(\d[\.,]?\d*)', rating_str.replace(',', '.'))
    if match:
        try: return float(match.group(1))
        except ValueError: return None
    return None
def clean_sold(sold_str):
    if not sold_str: return None
    cleaned = sold_str.lower().replace('đã bán', '').strip()
    num_part = re.search(r'(\d+[\.,]?\d*)', cleaned.replace(',', '.'))
    if not num_part: return None
    try:
        num = float(num_part.group(1))
        if 'k' in cleaned: num *= 1000
        return int(num)
    except ValueError: return None
def clean_percent(percent_str):
    if not percent_str: return None
    match = re.search(r'(\d+[\.,]?\d*)', percent_str.replace(',', '.'))
    if match:
        try: return float(match.group(1)) / 100.0 
        except ValueError: return None
    return None

def compare_rows(cleaned_row, target_row):
    """
    So sánh một dòng đã làm sạch từ nguồn với một dòng từ đích.
    Trả về True nếu CÓ THAY ĐỔI, False nếu KHÔNG.
    
    Lưu ý: Chúng ta chỉ so sánh các trường nghiệp vụ quan trọng.
    Các trường như COUPON, QUA_TANG... có thể bỏ qua nếu không muốn
    chúng kích hoạt một bản ghi lịch sử mới.
    """
    if (cleaned_row['TEN'] or '') != (target_row['TEN'] or ''): return True
    if (cleaned_row['LINK_ANH'] or '') != (target_row['LINK_ANH'] or ''): return True
    if (cleaned_row['GIA_CU'] or 0) != (target_row['GIA_CU'] or 0): return True
    if (cleaned_row['GIA_MOI'] or 0) != (target_row['GIA_MOI'] or 0): return True
    if (cleaned_row['KICH_THUOC_MAN_HINH'] or 0) != (target_row['KICH_THUOC_MAN_HINH'] or 0): return True
    if (cleaned_row['RAM'] or 0) != (target_row['RAM'] or 0): return True
    if (cleaned_row['BO_NHO'] or 0) != (target_row['BO_NHO'] or 0): return True
    if (cleaned_row['DANH_GIA'] or 0) != (target_row['DANH_GIA'] or 0): return True
    if (cleaned_row['DA_BAN'] or 0) != (target_row['DA_BAN'] or 0): return True
    
    return False

def main():
    print("--- Bắt đầu Bước 3: Transform Staging (SCD Type 2) ---")
    conn_control = None
    conn_staging = None
    
    current_time = datetime.datetime.now()
    
    count_new = 0
    count_updated = 0
    count_no_change = 0
    count_error = 0
    
    try:
        conn_control = mysql.connector.connect(**DB_CONTROL_CONFIG)
        conn_staging = mysql.connector.connect(**DB_STAGING_CONFIG)
        cursor_read = conn_staging.cursor(dictionary=True)
        cursor_write = conn_staging.cursor()

        cursor_read.execute("SELECT * FROM PRODUCTS_GENERAL")
        source_rows = cursor_read.fetchall()
        print(f">>>>>>>>> Đã đọc {len(source_rows)} sản phẩm từ 'PRODUCTS_GENERAL'.")

        cursor_read.execute("SELECT * FROM PRODUCTS_TRANSFORM WHERE EXPIRED_AT = '9999-12-31'")
        target_rows = cursor_read.fetchall()
        
        target_map = {}
        for row in target_rows:
            key = (row['ID_CONFIG'], row['LINK'])
            target_map[key] = row
        
        print(f">>>>>>>>> Đã đọc {len(target_map)} bản ghi đang hoạt động từ 'PRODUCTS_TRANSFORM'.")

        for source_row in source_rows:
            try:
                cleaned_row = {
                    'ID': source_row.get('ID'), 
                    'ID_CONFIG': source_row.get('ID_CONFIG'),
                    'TEN': source_row.get('TEN'),
                    'LINK': source_row.get('LINK'),
                    'LINK_ANH': source_row.get('LINK_ANH'),
                    'GIA_CU': clean_price(source_row.get('GIA_CU')),
                    'GIA_MOI': clean_price(source_row.get('GIA_MOI')),
                    'KICH_THUOC_MAN_HINH': clean_spec(source_row.get('KICH_THUOC_MAN_HINH'), "INCH"),
                    'RAM': clean_spec(source_row.get('RAM'), "GB"),
                    'BO_NHO': clean_spec(source_row.get('BO_NHO'), "GB"),
                    'GIAM_GIA_SMEMBER': clean_price(source_row.get('GIAM_GIA_SMEMBER')),
                    'GIAM_GIA_SSTUDENT': clean_price(source_row.get('GIAM_GIA_SSTUDENT')),
                    'GIAM_GIA_PHAN_TRAM': clean_percent(source_row.get('GIAM_GIA_PHAN_TRAM')),
                    'COUPON': source_row.get('COUPON'),
                    'QUA_TANG': source_row.get('QUA_TANG'),
                    'DANH_GIA': clean_rating(source_row.get('DANH_GIA')),
                    'DA_BAN': clean_sold(source_row.get('DA_BAN'))
                }
                
                key = (cleaned_row['ID_CONFIG'], cleaned_row['LINK'])
                target_row = target_map.get(key)
                
                sql_insert = """
                    INSERT INTO PRODUCTS_TRANSFORM
                    (ID, ID_CONFIG, TEN, LINK, LINK_ANH, GIA_CU, GIA_MOI,
                     KICH_THUOC_MAN_HINH, RAM, BO_NHO, GIAM_GIA_SMEMBER,
                     GIAM_GIA_SSTUDENT, GIAM_GIA_PHAN_TRAM, COUPON, QUA_TANG,
                     DANH_GIA, DA_BAN, CREATED_AT, UPDATED_AT, EXPIRED_AT)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                insert_data = (
                    cleaned_row['ID'], cleaned_row['ID_CONFIG'], cleaned_row['TEN'], cleaned_row['LINK'],
                    cleaned_row['LINK_ANH'], cleaned_row['GIA_CU'], cleaned_row['GIA_MOI'],
                    cleaned_row['KICH_THUOC_MAN_HINH'], cleaned_row['RAM'], cleaned_row['BO_NHO'],
                    cleaned_row['GIAM_GIA_SMEMBER'], cleaned_row['GIAM_GIA_SSTUDENT'],
                    cleaned_row['GIAM_GIA_PHAN_TRAM'], cleaned_row['COUPON'], cleaned_row['QUA_TANG'],
                    cleaned_row['DANH_GIA'], cleaned_row['DA_BAN'],
                    current_time, 
                    current_time, 
                    '9999-12-31' 
                )

                if not target_row:
                    cursor_write.execute(sql_insert, insert_data)
                    count_new += 1
                else:
                    if compare_rows(cleaned_row, target_row):
                        sql_update = "UPDATE PRODUCTS_TRANSFORM SET EXPIRED_AT = %s WHERE ID_SR = %s"
                        cursor_write.execute(sql_update, (current_time, target_row['ID_SR']))
                        
                        cursor_write.execute(sql_insert, insert_data)
                        count_updated += 1
                    else:
                        count_no_change += 1
                        pass
                        
            except Exception as e:
                print(f"Lỗi khi xử lý sản phẩm (LINK: {source_row.get('LINK')}): {e}")
                log_message(conn_control, source_row.get('ID_CONFIG'), "TRANSFORM_FAIL", rows=0, 
                            details_dict={"error": "Lỗi xử lý SCD 2", "details": str(e), "product_name": source_row.get('TEN')})
                count_error += 1

        source_keys = set((r['ID_CONFIG'], r['LINK']) for r in source_rows)
        target_keys = set(target_map.keys())
        
        missing_keys = target_keys - source_keys
        if missing_keys:
            print(f">>>>>>>>> Đang xử lý {len(missing_keys)} sản phẩm 'biến mất' (không còn bán)...")
            for key in missing_keys:
                target_row = target_map[key]
                sql_update = "UPDATE PRODUCTS_TRANSFORM SET EXPIRED_AT = %s WHERE ID_SR = %s"
                cursor_write.execute(sql_update, (current_time, target_row['ID_SR']))
        
        total_rows_processed = count_new + count_updated
        log_details = {
            "new": count_new,
            "updated": count_updated,
            "no_change": count_no_change,
            "expired": len(missing_keys),
            "errors": count_error
        }
        log_message(conn_control, None, "TRANSFORM_SUCCESS", rows=total_rows_processed, details_dict=log_details)
        
        conn_staging.commit() 
        
        print(f"\n--- HOÀN TẤT ---")
        print(f">>> Mới: {count_new} | Cập nhật: {count_updated} | Không đổi: {count_no_change} | Hết hạn: {len(missing_keys)} | Lỗi: {count_error}")
            
    except Exception as e:
        if conn_staging: conn_staging.rollback() 
        if conn_control: log_message(conn_control, None, "TRANSFORM_FAIL", rows=0, details_dict={"error": "Lỗi nghiêm trọng", "details": str(e)})
        print(f"Lỗi nghiêm trọng khi transform: {e}")
    finally:
        if 'cursor_read' in locals() and cursor_read: cursor_read.close()
        if 'cursor_write' in locals() and cursor_write: cursor_write.close()
        if conn_staging: conn_staging.close()
        if conn_control: conn_control.close()
        print(">>>>>>>>> Đã đóng kết nối DB.")

if __name__ == "__main__":
    main()