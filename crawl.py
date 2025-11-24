from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import mysql.connector
import time
import csv
import os
import configparser
import datetime
import glob
import shutil
import json

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

DB_CONTROL_CONFIG = config['database_control']

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(CURRENT_DIR, "backup")
TODAY_STR = datetime.date.today().strftime("%Y_%m_%d")
CSV_FILE_NAME = f"products_raw_{TODAY_STR}.csv"
CSV_FILE_PATH = os.path.join(CURRENT_DIR, CSV_FILE_NAME)

csv_headers = [
    'ID', 'TEN', 'LINK', 'LINK_ANH', 'GIA_CU', 'GIA_MOI', 'KICH_THUOC_MAN_HINH',
    'RAM', 'BO_NHO', 'GIAM_GIA_SMEMBER', 'GIAM_GIA_SSTUDENT', 'GIAM_GIA_PHAN_TRAM',
    'COUPON', 'QUA_TANG', 'DANH_GIA', 'DA_BAN', 
    'SITE_NAME', 'SITE_ID'
]

def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONTROL_CONFIG)
    except Exception as e:
        print(f"!!! Lỗi kết nối DB: {e}")
        return None

def get_general_config(key, default=None):
    if key == "CHROME_DRIVER_PATH": return r"D:\Driver\chromedriver.exe" 
    if key == "HEADLESS": return "1"
    if key == "CRAWL_TIMEOUT": return "10"
    if key == "MAX_SHOW_MORE": return "5"
    if key == "MAX_PRODUCTS": return "200"
    return default

def start_crawl_log(conn, config_id, site_name):
    try:
        cursor = conn.cursor()
        
        check_success_sql = """
            SELECT ID FROM crawl_log 
            WHERE ID_CONFIG = %s AND STATUS = 'SUCCESS' 
            AND DATE(RUN_DATE) = CURDATE()
        """
        cursor.execute(check_success_sql, (config_id,))
        if cursor.fetchone():
            print(f">>> Site {site_name} (ID: {config_id}) đã cào THÀNH CÔNG hôm nay. Bỏ qua.")
            return None 

        check_running_sql = """
            SELECT ID FROM crawl_log 
            WHERE ID_CONFIG = %s AND STATUS = 'RUNNING' 
            AND DATE(RUN_DATE) = CURDATE()
        """
        cursor.execute(check_running_sql, (config_id,))
        if cursor.fetchone():
            print(f"!!! Site {site_name} (ID: {config_id}) đang ở trạng thái RUNNING. Bỏ qua.")
            return None

        insert_sql = """
            INSERT INTO crawl_log (ID_CONFIG, SITE_NAME, STATUS, RUN_DATE) 
            VALUES (%s, %s, 'RUNNING', NOW())
        """
        cursor.execute(insert_sql, (config_id, site_name))
        conn.commit()
        log_id = cursor.lastrowid
        cursor.close()
        return log_id
    except Exception as e:
        print(f"Lỗi khi khởi tạo log: {e}")
        return None

def update_crawl_log(conn, log_id, status, file_path=None, rows=0, error_msg=None):
    if not log_id: return
    try:
        cursor = conn.cursor()
        sql = """
            UPDATE crawl_log 
            SET STATUS = %s, FILE_PATH = %s, ROWS_AFFECTED = %s, ERROR_MESSAGE = %s
            WHERE ID = %s
        """
        cursor.execute(sql, (status, file_path, rows, error_msg, log_id))
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Lỗi khi cập nhật log: {e}")

def move_old_csv_files():
    try:
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
        
        old_files = glob.glob(os.path.join(CURRENT_DIR, "products_raw_*.csv"))
        moved_count = 0
        for file_path in old_files:
            if os.path.basename(file_path) != CSV_FILE_NAME:
                shutil.move(file_path, os.path.join(BACKUP_DIR, os.path.basename(file_path)))
                moved_count += 1
        
        if moved_count > 0:
            print(f">>>>>>>>> Đã backup {moved_count} file CSV cũ.")
    except Exception as e:
        print(f"Lỗi khi backup file: {e}")
        
def crawl_one_site(site_row, writer, conn_control, start_id):
    SITE_NAME = site_row['TEN']
    SITE_ID = site_row['ID']
    URL = site_row['URL']
    
    print(f"\n--- BẮT ĐẦU QUY TRÌNH CHO {SITE_NAME} ---")

    log_id = start_crawl_log(conn_control, SITE_ID, SITE_NAME)
    if not log_id: return 0 

    try:
        SEL_PRODUCT_CONTAINER = site_row['THE_PRODUCT_CONTAINER']
        if not URL or not SEL_PRODUCT_CONTAINER: raise Exception("Thiếu URL hoặc Selector")

        options = Options()
        if get_general_config("HEADLESS") == "1": options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("window-size=1920,1080")
        options.add_argument("--disable-dev-shm-usage") 
        options.add_argument("--log-level=3") 
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
        options.set_capability("pageLoadStrategy", "eager")

        service = Service(get_general_config("CHROME_DRIVER_PATH"))
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)

        try:
            print(f">>> Đang truy cập: {URL}...")
            driver.get(URL)
            WebDriverWait(driver, int(get_general_config("CRAWL_TIMEOUT"))).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, SEL_PRODUCT_CONTAINER)) > 0
            )
            print(f">>>>>>>>> Đã load xong khung HTML.")
            
            click_count = 0
            max_show = int(get_general_config("MAX_SHOW_MORE"))
            btn_sel = site_row['THE_BTN_SHOW_MORE']
            
            if btn_sel:
                while click_count < max_show:
                    try:
                        btn = driver.find_element(By.CSS_SELECTOR, btn_sel)
                        driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                        time.sleep(1) 
                        driver.execute_script("arguments[0].click();", btn)
                        print(f">>>>>>>>> Đã click 'Xem thêm' lần {click_count + 1}")
                        time.sleep(3)
                        click_count += 1
                    except: break
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            products = soup.select(SEL_PRODUCT_CONTAINER)
            
            if not products:
                raise Exception(f"Không tìm thấy sản phẩm với selector: {SEL_PRODUCT_CONTAINER}")

            site_products = []
            current_id = start_id
            
            for item in products:
                try:
                    get_text = lambda sel: item.select_one(sel).get_text(strip=True) if sel and item.select_one(sel) else ""
                    name = get_text(site_row['THE_TEN_SP'])
                    if not name: continue
                    link = get_text(site_row['THE_LINK'])
                    if not link.startswith("http"):
                        if SITE_NAME == "TGDD": link = "https://www.thegioididong.com" + link
                        elif SITE_NAME == "CELLPHONES" and link.startswith("/"): link = "https://cellphones.com.vn" + link
                    img_link = ""
                    img_tag = item.select_one(site_row['THE_LINK_ANH'])
                    if img_tag: img_link = img_tag.get("src") or img_tag.get("data-src") or ""
                    price_new = get_text(site_row['THE_GIA_MOI'])
                    price_old = get_text(site_row['THE_GIA_CU'])
                    
                    size = ram = memory = ""
                    if SITE_NAME == "CELLPHONES":
                        badges = item.select(site_row['THE_KICH_THUOC_MAN_HINH']) 
                        if len(badges) > 0: size = badges[0].get_text(strip=True)
                        if len(badges) > 1: ram = badges[1].get_text(strip=True)
                        if len(badges) > 2: memory = badges[2].get_text(strip=True)
                    elif SITE_NAME == "TGDD":
                        if site_row['THE_KICH_THUOC_MAN_HINH']:
                            for b in item.select(site_row['THE_KICH_THUOC_MAN_HINH']):
                                t = b.get_text(strip=True)
                                if '"' in t or "inch" in t.lower(): size = t; break
                        if site_row['THE_RAM']:
                            mem_tag = item.select_one(site_row['THE_RAM'])
                            if mem_tag:
                                parts = mem_tag.get_text(strip=True).split(" - ")
                                if len(parts) == 2: ram, memory = parts
                                elif len(parts) == 1:
                                    val = parts[0].upper().replace("GB", "").strip()
                                    try: num = int(val)
                                    except: num = 0
                                    if num >= 64: memory = parts[0].strip(); ram = ""
                                    else: ram = parts[0].strip(); memory = ""

                    site_products.append({
                        'ID': current_id, 'TEN': name, 'LINK': link, 'LINK_ANH': img_link,
                        'GIA_CU': price_old, 'GIA_MOI': price_new,
                        'KICH_THUOC_MAN_HINH': size, 'RAM': ram, 'BO_NHO': memory,
                        'GIAM_GIA_SMEMBER': get_text(site_row['THE_GIAM_GIA_SMEMBER']),
                        'GIAM_GIA_SSTUDENT': get_text(site_row['THE_GIAM_GIA_SSTUDENT']),
                        'GIAM_GIA_PHAN_TRAM': get_text(site_row['THE_GIAM_GIA_PHAN_TRAM']),
                        'COUPON': get_text(site_row['THE_COUPON']),
                        'QUA_TANG': get_text(site_row['THE_QUA_TANG']),
                        'DANH_GIA': get_text(site_row['THE_DANH_GIA']),
                        'DA_BAN': get_text(site_row['THE_DA_BAN']),
                        'SITE_NAME': SITE_NAME, 'SITE_ID': SITE_ID
                    })
                    current_id += 1
                except: pass

            if site_products:
                writer.writerows(site_products)
                count = len(site_products)
                update_crawl_log(conn_control, log_id, "SUCCESS", file_path=CSV_FILE_PATH, rows=count)
                print(f">>>>>>>>> {SITE_NAME}: Thành công ({count} sản phẩm)")
                return count
            else:
                raise Exception("Không trích xuất được dữ liệu nào")
        finally:
            driver.quit()
    except Exception as e:
        print(f"!!! Lỗi {SITE_NAME}: {e}")
        update_crawl_log(conn_control, log_id, "FAILED", rows=0, error_msg=str(e))
        return 0

def main():
    print("--- Bắt đầu Quy trình CRAWL ---")
    move_old_csv_files()
    
    conn = get_db_connection()
    if not conn: return

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM config")
        configs = cursor.fetchall()
        
        if not configs:
            print("Lỗi: Bảng config rỗng.")
            return

        sites_need_crawling = False
        for cfg in configs:
            sql_check = "SELECT ID FROM crawl_log WHERE ID_CONFIG=%s AND STATUS='SUCCESS' AND DATE(RUN_DATE)=CURDATE()"
            cursor.execute(sql_check, (cfg['ID'],))
            if not cursor.fetchone():
                sites_need_crawling = True 
                break
        
        if not sites_need_crawling:
            print(">>> TOÀN BỘ WEBSITE ĐÃ ĐƯỢC CÀO THÀNH CÔNG HÔM NAY.")
            print(">>> Giữ nguyên file CSV hiện tại. Kết thúc chương trình.")
            return

        with open(CSV_FILE_PATH, mode='w', newline='', encoding='utf-8-sig') as file:
            writer = csv.DictWriter(file, fieldnames=csv_headers)
            writer.writeheader()
            
            global_id = 1
            for config_row in configs:
                count = crawl_one_site(config_row, writer, conn, global_id)
                global_id += count

        print("\n--- HOÀN TẤT TOÀN BỘ ---")

    except Exception as e:
        print(f"Lỗi Main: {e}")
    finally:
        if conn.is_connected(): conn.close()

if __name__ == "__main__":
    main()