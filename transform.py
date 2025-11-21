import mysql.connector
from mysql.connector import Error as MySQLError
import sys
import re
import configparser
import os
import datetime

def load_sql_commands(cursor):
    sql_commands = {}
    try:
        cursor.execute("SELECT COMMAND_NAME, SQL_QUERY FROM SQL_COMMANDS")
        # Vẫn không hiểu tại sao không dùng for (name, query) on cursor duyệt qua dic
        for row in cursor:
            name = row['COMMAND_NAME']
            query = row['SQL_QUERY']
            #print(name)
            sql_commands[name] = query
        return sql_commands
    except Exception as e:
        print(f"LỖI: Không thể tải các lệnh SQL từ db_control. Chi tiết: {e}")
        raise

def execute_sp_definition(cursor, query_name):
    sp_query = SQL_COMMANDS.get(query_name)
    if not sp_query:
        raise Exception(f"LỖI: Không tìm thấy lệnh SQL: {query_name}")
    sp_name_only = query_name
    try:
        cursor.execute(f"DROP PROCEDURE IF EXISTS {sp_name_only}")
        sp_create_command = re.search(r'(CREATE\s+PROCEDURE.*)', sp_query, re.DOTALL | re.IGNORECASE)
        if not sp_create_command:
            raise Exception("Lỗi: Không tìm thấy 'CREATE PROCEDURE' trong định nghĩa SQL.")
        for result in cursor.execute(sp_create_command.group(0), multi=True):
            pass
        print(f"Dùng {query_name} với phương pháp multi.")
        return

    except (TypeError, MySQLError) as e:
        if 'unexpected keyword argument \'multi\'' in str(e) or 'You have an error in your SQL syntax' in str(e):
            pass
        else:
            raise MySQLError(f"Lỗi SQL trong Phương pháp 1: {e}")
    try:
        match = re.search(r'(\bCREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE.*?END\s*\$\$)', sp_query, re.DOTALL | re.IGNORECASE)
        if not match:
            raise Exception("Lỗi Regex: Không tìm thấy khối 'CREATE PROCEDURE...END$$'.")
        full_sp_text = match.group(1)
        cleaned_sp = re.sub(r'[^\x20-\x7E\t\n\r]+', ' ', full_sp_text).strip()
        final_query = cleaned_sp.replace('$$', ';')
        cursor.execute(f"DROP PROCEDURE IF EXISTS {sp_name_only}")
        cursor.execute(final_query)
        print(f"Dùng {query_name} Phương pháp đơn.")
    except MySQLError as err:
        raise MySQLError(f"Lỗi SQL khi cố gắng tạo SP (Sau khi lỗi 'multi'): {err.msg}")
    except Exception as e:
        if "Lỗi Regex" in str(e):
            raise
        raise Exception(f"Lỗi không xác định khi tạo SP: {e}")


config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path, encoding='utf-8-sig')
try:
    DB_CONTROL_CONFIG = config['database_control']
    DB_STAGING_CONFIG = config['database_staging']
except KeyError as e:
    print(f"Lỗi: Không tìm thấy section cấu hình {e} trong file config.ini.")
    exit()

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(CURRENT_DIR, "backup") 
TODAY_STR = datetime.date.today().strftime("%Y_%m_%d")
CSV_FILE_NAME = f"products_raw_{TODAY_STR}.csv"
CSV_FILE_PATH = os.path.join(CURRENT_DIR, CSV_FILE_NAME)

try:
    # 1. Kết nối với Control DB
    conn_control = mysql.connector.connect(**DB_CONTROL_CONFIG)
    cursor_control = conn_control.cursor(dictionary=True)
    print(f"Kết nối Database Control ({DB_CONTROL_CONFIG.get('database')}) thành công.")
    conn_staging = mysql.connector.connect(**DB_STAGING_CONFIG)
    cursor_staging = conn_staging.cursor()
    print(f"Kết nối Database Staging ({DB_STAGING_CONFIG.get('database')}) thành công.")
    print(">>>>>>>>>>>> BƯỚC 1: Kết nối DB")

    #2 Load lệnh.
    CMD_CLEAN_DATA = 'SP_ETL_CLEAN_DATA'
    CMD_SCD_UPDATE = 'SP_ETL_SCD_UPDATE_PRODUCT'
    CMD_UPDATE_LOG = 'SP_ETL_UPDATE_LOG_STATUS'
    CMD_COUNT_PROCESS_LOG = 'COUNT_RUNNING_PROCESS_LOG'
    CMD_COUNT_ETL_LOG = 'COUNT_RUNNING_ETL_LOG'
    CMD_EXEC_SCD = 'SP_ETL_PRODUCT_SCD_EXEC'
    CMD_SELECT_PROCESS_ID = 'SELECT_PROCESS_ID'
    CMD_INSERT_PROCESS_LOG_RUNNING = 'INSERT_PROCESS_LOG_RUNNING'

    SQL_COMMANDS = load_sql_commands(cursor_control)
    SP_CLEAN_DATA_QUERY = SQL_COMMANDS[CMD_CLEAN_DATA]
    SP_SCD_UPDATE_QUERY = SQL_COMMANDS[CMD_SCD_UPDATE]
    SP_UPDATE_LOG_STATUS = SQL_COMMANDS[CMD_UPDATE_LOG]
    COUNT_PROCESS_LOG_QUERY = SQL_COMMANDS[CMD_COUNT_PROCESS_LOG]
    COUNT_ETL_LOG_QUERY = SQL_COMMANDS[CMD_COUNT_ETL_LOG]
    SP_EXEC_SCD_QUERY = SQL_COMMANDS[CMD_EXEC_SCD]
    SELECT_PROCESS_ID_QUERY = SQL_COMMANDS[CMD_SELECT_PROCESS_ID]
    INSERT_LOG_RUNNING_QUERY = SQL_COMMANDS[CMD_INSERT_PROCESS_LOG_RUNNING]
    try:
        execute_sp_definition(cursor_staging, CMD_CLEAN_DATA)
        conn_staging.commit()
    except Exception as e:
        print(f"LỖI TẠO SP {CMD_CLEAN_DATA}: {e}")
        raise
    try:
        execute_sp_definition(cursor_staging, CMD_SCD_UPDATE)
        conn_staging.commit()
    except Exception as e:
        print(f"LỖI TẠO SP {CMD_SCD_UPDATE}: {e}")
        raise
    try:
        execute_sp_definition(cursor_control, CMD_UPDATE_LOG)
        conn_staging.commit()
    except Exception as e:
        print(f"LỖI TẠO SP {CMD_UPDATE_LOG}: {e}")
        raise
    print(">>>>>>>>>>>> BƯỚC 2: Tải lệnh thành công")

    # 3. Kiểm tra critical section.
    cursor_control.execute(COUNT_PROCESS_LOG_QUERY)
    running_in_process_log = cursor_control.fetchone()['running_count']
    cursor_control.execute(COUNT_ETL_LOG_QUERY)
    running_in_etl_log = cursor_control.fetchone()['running_count']
    running_count = running_in_process_log + running_in_etl_log
    if running_count > 0:
        print(f"Đang có {running_count} tiến trình đang ở trạng thái 'Running' (trong PROCESS_LOG: {running_in_process_log}, trong ETL_LOG: {running_in_etl_log}).")
        sys.exit(0)

    print(">>>>>>>>>>>> BƯỚC 3: Kiểm tra Critical Section (Không có tiến trình nào đang chạy)")

    # 4. Tạo log trong PROCESS_LOG
    process_name_to_log = 'Transform_Process'
    cursor_control.execute(SELECT_PROCESS_ID_QUERY, (process_name_to_log,))
    result = cursor_control.fetchone()
    if result:
        process_id = result['ID']
    else:
        raise Exception(f"LỖI: Không tìm thấy Process có tên: {process_name_to_log} trong bảng PROCESS.")
    cursor_control.execute(INSERT_LOG_RUNNING_QUERY, (process_id, 'Running'))
    conn_control.commit()
    process_log_id = cursor_control.lastrowid
    print(">>>>>>>>>>>> BƯỚC 4: Ghi Log Bắt đầu Tiến trình vào PROCESS_LOG")

    # 5. Làm sạch dữ liệu.
    call_clean_data = f"CALL {CMD_CLEAN_DATA}()"
    try:
        cursor_staging.execute(call_clean_data) 
        conn_staging.commit()
        print(f"-> Đã thực thi SP {CMD_CLEAN_DATA} thành công.")
    except MySQLError as err:
        print(f"LỖI SQL khi chạy SP Clean Data: {err}")
        cursor_control.execute(f"CALL {CMD_UPDATE_LOG}(%s, %s, %s, %s, %s)", (process_log_id, 0, 0, 0, 'Failed'))
        conn_control.commit()
        raise
    print(">>>>>>>>>>>> BƯỚC 5: Làm sạch dữ liệu và thêm vào PRODUCT_TRANSFORM")

    # 6. Thêm sản phẩm mới.
    # 7. Tìm khóa thay đổi.
    # 8. Đánh dấu hết hạn.
    # 9. Thêm phiên bản mới.
    call_scd = f"CALL {CMD_SCD_UPDATE}(@input_rows, @inserted_rows, @updated_rows)"
    try:
        cursor_staging.execute(call_scd)
        conn_staging.commit()
        cursor_staging.execute("SELECT @input_rows, @inserted_rows, @updated_rows")
        log_stats = cursor_staging.fetchone()
        RowsInput = log_stats[0]
        RowsInserted = log_stats[1]
        RowsUpdated = log_stats[2]
        print(f"Kết quả -> Dòng xử lý: {RowsInput} | Dòng chèn mới: {RowsInserted} | Dòng hết hạn: {RowsUpdated}")
        log_params = (process_log_id, RowsInput, RowsInserted, RowsUpdated, 'Success')
        cursor_control.execute(f"CALL {CMD_UPDATE_LOG}(%s, %s, %s, %s, %s)", log_params)
        conn_control.commit()
    except MySQLError as err:
        print(f"LỖI SQL khi chạy SP SCD Update: {err}")
        cursor_control.execute(f"CALL {CMD_UPDATE_LOG}(%s, %s, %s, %s, %s)", (process_log_id, 0, 0, 0, 'Failed'))
        conn_control.commit()
        raise
    print(">>>>>>>>>>>> BƯỚC 6, 7, 8, 9, 10")

except MySQLError as err:
    print(f"LỖI KẾT NỐI hoặc SQL: {err}")
    if conn_control and conn_control.is_connected():
        conn_control.close()
    if conn_staging and conn_staging.is_connected():
        conn_staging.close()
    sys.exit(1)
except Exception as e:
    print(f"LỖI KHÔNG XÁC ĐỊNH: {e}")
    if conn_control and conn_control.is_connected():
        conn_control.close()
    if conn_staging and conn_staging.is_connected():
        conn_staging.close()
    sys.exit(1)
finally:
    if conn_control and conn_control.is_connected():
        cursor_control.close()
        conn_control.close()
        print(">>> Đã đóng kết nối Control DB.")
    if conn_staging and conn_staging.is_connected():
        cursor_staging.close()
        conn_staging.close()
        print(">>> Đã đóng kết nối Staging DB.")