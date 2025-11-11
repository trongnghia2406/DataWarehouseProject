import mysql.connector
import configparser
import os
import datetime

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

DB_WAREHOUSE_CONFIG = config['database_warehouse']

def get_quarter_start(date_obj):
    """Tìm ngày bắt đầu của quý"""
    quarter_month = ((date_obj.month - 1) // 3) * 3 + 1
    return datetime.date(date_obj.year, quarter_month, 1)

def main():
    print("--- Bắt đầu nạp dữ liệu cho DIM_DATE ---")
    conn_warehouse = None
    
    try:
        conn_warehouse = mysql.connector.connect(**DB_WAREHOUSE_CONFIG)
        cursor = conn_warehouse.cursor()

        print(">>>>>>>>> 1. Tạm thời tắt kiểm tra khóa ngoại (Foreign Key Checks)...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

        cursor.execute("TRUNCATE TABLE FACT_PRODUCT_SALES")
        print(">>>>>>>>> 2. Đã xóa dữ liệu cũ trong FACT_PRODUCT_SALES.")
        cursor.execute("TRUNCATE TABLE DIM_DATE")
        print(">>>>>>>>> 3. Đã xóa dữ liệu cũ trong DIM_DATE.")
        
        print(">>>>>>>>> 4. Bật lại kiểm tra khóa ngoại...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        start_date = datetime.date(2020, 1, 1)
        end_date = datetime.date(2030, 12, 31)
        
        current_date = start_date
        dates_to_insert = []
        
        print(f">>>>>>>>> 5. Đang tạo dữ liệu ngày từ {start_date} đến {end_date}...")

        while current_date <= end_date:
            full_date = current_date
            int_day = current_date.day
            int_month = current_date.month
            calendar_year = current_date.year
            date_of_month = current_date.day
            day_of_year = current_date.timetuple().tm_yday
            
            day_of_week_num = current_date.weekday()
            day_of_week_name = ['Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật'][day_of_week_num]
            
            calendar_month_name = f"Tháng {int_month}"
            calendar_year_month = f"{calendar_year}-{int_month:02d}"
            
            sun_week_start = current_date - datetime.timedelta(days=(day_of_week_num + 1) % 7)
            sun_week_of_year = (sun_week_start - datetime.date(calendar_year, 1, 1)).days // 7 + 1
            sun_year_week = f"{calendar_year}-W{sun_week_of_year:02d} (Sun)"

            mon_week_start = current_date - datetime.timedelta(days=day_of_week_num)
            mon_week_of_year = (mon_week_start - datetime.date(calendar_year, 1, 1)).days // 7 + 1
            mon_year_week = f"{calendar_year}-W{mon_week_of_year:02d} (Mon)"
            
            quarter_num = (int_month - 1) // 3 + 1
            quarter_name = f"Q{quarter_num}"
            quarter_start_date = get_quarter_start(current_date)
            
            holiday_name = "Không" 
            day_type = "Cuối tuần" if day_of_week_num >= 5 else "Ngày thường"

            dates_to_insert.append((
                full_date, int_day, int_month, day_of_week_name, calendar_month_name,
                calendar_year, calendar_year_month, date_of_month, day_of_year,
                sun_week_of_year, sun_year_week, sun_week_start,
                mon_week_of_year, mon_year_week, mon_week_start,
                quarter_name, quarter_start_date, holiday_name, day_type
            ))
            
            current_date += datetime.timedelta(days=1)

        print(f">>>>>>>>> 6. Đang nạp {len(dates_to_insert)} bản ghi vào DB...")
        sql_insert = """
            INSERT INTO DIM_DATE (
                FULL_DATE, INT_DAY, INT_MONTH, DAY_OF_WEEK, CALENDAR_MONTH,
                CALENDAR_YEAR, CALENDAR_YEAR_MONTH, DATE_OF_MONTH, DAY_OF_YEAR,
                SUN_WEEK_OF_YEAR, SUN_YEAR_WEEK, SUN_WEEK,
                MON_WEEK_OF_YEAR, MON_YEAR_WEEK, MON_WEEK,
                `QUARTER`, QUARTER_OF_YEAR, HOLIDAY, DAY_TYPE
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(sql_insert, dates_to_insert)
        conn_warehouse.commit()
        
        print(f"\n--- HOÀN TẤT ---")
        print(f">>>>>>>>> Đã nạp {len(dates_to_insert)} ngày vào 'DIM_DATE'.")

    except Exception as e:
        if conn_warehouse: conn_warehouse.rollback()
        print(f"Lỗi nghiêm trọng khi nạp DIM_DATE: {e}")
    finally:
        try:
            if conn_warehouse and cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
                print(">>>>>>>>> Đã khôi phục kiểm tra khóa ngoại.")
        except:
            pass

        if 'cursor' in locals() and cursor: cursor.close()
        if conn_warehouse: conn_warehouse.close()
        print(">>>>>>>>> Đã đóng kết nối DB 'db_warehouse'.")

if __name__ == "__main__":
    main()