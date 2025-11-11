from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
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

try:
    conn_control = mysql.connector.connect(**DB_CONTROL_CONFIG)
    cursor_control = conn_control.cursor(dictionary=True) 
    print(">>>>>>>>> Kết nối DB 'db_control' thành công")
except mysql.connector.Error as err:
    print(f"Lỗi kết nối DB 'db_control': {err}")
    exit()

def get_general_config(key, default=None):
    if key == "CHROME_DRIVER_PATH":
        return r"D:\Driver\chromedriver.exe" 
    if key == "HEADLESS": return "1"
    if key == "CRAWL_TIMEOUT": return "10"
    if key == "MAX_SHOW_MORE": return "5"
    if key == "MAX_PRODUCTS": return "200"
    return default

def log_message(conn, config_id, status, rows=None, details_dict=None):
    """
    Ghi log có cấu trúc vào etl_log.
    details_dict sẽ được chuyển thành chuỗi JSON.
    """
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

def move_old_csv_files():
    """Tạo folder backup và di chuyển file CSV cũ vào đó"""
    try:
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
            print(f">>>>>>>>> Đã tạo thư mục '{BACKUP_DIR}'")
        
        old_files = glob.glob(os.path.join(CURRENT_DIR, "products_raw_*.csv"))
        
        moved_count = 0
        for file_path in old_files:
            if os.path.basename(file_path) != CSV_FILE_NAME:
                shutil.move(file_path, os.path.join(BACKUP_DIR, os.path.basename(file_path)))
                moved_count += 1
        
        if moved_count > 0:
            print(f">>>>>>>>> Đã di chuyển {moved_count} file CSV cũ vào thư mục 'backup'.")

    except Exception as e:
        print(f"Lỗi khi di chuyển file CSV cũ: {e}")


csv_headers = [
    'ID', 'TEN', 'LINK', 'LINK_ANH', 'GIA_CU', 'GIA_MOI', 'KICH_THUOC_MAN_HINH',
    'RAM', 'BO_NHO', 'GIAM_GIA_SMEMBER', 'GIAM_GIA_SSTUDENT', 'GIAM_GIA_PHAN_TRAM',
    'COUPON', 'QUA_TANG', 'DANH_GIA', 'DA_BAN', 
    'SITE_NAME', 'SITE_ID'
]

try:
    move_old_csv_files()

    print(f">>>>>>>>> Sẽ ghi vào file của hôm nay: '{CSV_FILE_PATH}'")

    with open(CSV_FILE_PATH, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=csv_headers)
        writer.writeheader() 
        
        cursor_control.execute("SELECT * FROM config")
        site_configs = cursor_control.fetchall()
        
        if not site_configs:
            print("!!! LỖI: Bảng 'config' trong 'db_control' bị rỗng.")
            log_message(conn_control, None, "CRAWL_FAIL", rows=0, details_dict={"error": "Bảng config rỗng"})
            exit()

        print(f">>>>>>>>> Tìm thấy {len(site_configs)} site trong DB để cào.")

        DRIVER_PATH = get_general_config("CHROME_DRIVER_PATH")
        HEADLESS = get_general_config("HEADLESS") == "1"
        TIMEOUT = int(get_general_config("CRAWL_TIMEOUT", "10"))
        MAX_SHOW_MORE = int(get_general_config("MAX_SHOW_MORE", "5"))
        MAX_PRODUCTS = int(get_general_config("MAX_PRODUCTS", "200"))

        product_counter_id = 1 

        for site_row in site_configs:
            SITE_NAME = site_row['TEN']
            SITE_ID = site_row['ID']
            URL = site_row['URL']
            
            SEL_PRODUCT_CONTAINER = site_row['THE_PRODUCT_CONTAINER'] 
            SEL_NAME = site_row['THE_TEN_SP']
            SEL_LINK = site_row['THE_LINK']
            SEL_IMG = site_row['THE_LINK_ANH']
            SEL_PRICE_NEW = site_row['THE_GIA_MOI']
            SEL_PRICE_OLD = site_row['THE_GIA_CU']
            SEL_SCREEN = site_row['THE_KICH_THUOC_MAN_HINH']
            SEL_RAM = site_row['THE_RAM']
            SEL_MEM = site_row['THE_BO_NHO']
            SEL_DISCOUNT_SMEMBER = site_row['THE_GIAM_GIA_SMEMBER']
            SEL_DISCOUNT_STUDENT = site_row['THE_GIAM_GIA_SSTUDENT']
            SEL_DISCOUNT_PERCENT = site_row['THE_GIAM_GIA_PHAN_TRAM']
            SEL_COUPON = site_row['THE_COUPON']
            SEL_GIFT = site_row['THE_QUA_TANG']
            SEL_RATE = site_row['THE_DANH_GIA']
            SEL_SOLD_COUNT = site_row['THE_DA_BAN']
            SEL_BTN_SHOW_MORE = site_row['THE_BTN_SHOW_MORE']
            
            print(f"\n--- BẮT ĐẦU CRAWL {SITE_NAME} (ID: {SITE_ID}) ---")
            
            try:
                if not URL:
                    print(f">>>>>>> Lỗi: URL rỗng cho {SITE_NAME}. Bỏ qua...")
                    log_message(conn_control, SITE_ID, "CRAWL_FAIL", rows=0, details_dict={"error": f"URL rỗng cho {SITE_NAME}"})
                    continue 
                
                if not SEL_PRODUCT_CONTAINER:
                    print(f">>>>>>> Lỗi: THE_PRODUCT_CONTAINER rỗng cho {SITE_NAME}. Bỏ qua...")
                    log_message(conn_control, SITE_ID, "CRAWL_FAIL", rows=0, details_dict={"error": f"PRODUCT_CONTAINER rỗng cho {SITE_NAME}"})
                    continue

                options = Options()
                if HEADLESS: options.add_argument("--headless")
                options.add_argument("--disable-gpu"); options.add_argument("--no-sandbox")
                options.add_argument("window-size=1920,1080"); service = Service(DRIVER_PATH)
                driver = webdriver.Chrome(service=service, options=options)

                try:
                    driver.get(URL)
                    WebDriverWait(driver, TIMEOUT).until(
                        lambda d: len(d.find_elements(By.CSS_SELECTOR, SEL_PRODUCT_CONTAINER)) > 0
                    )
                    print(f">>>>>>>>> Đã load trang: {URL}")
                except Exception as e:
                    log_message(conn_control, SITE_ID, "CRAWL_FAIL", rows=0, details_dict={"error": "Không load được sản phẩm ban đầu", "details": str(e)})
                    driver.quit(); continue 

                click_count = 0
                while click_count < MAX_SHOW_MORE and SEL_BTN_SHOW_MORE:
                    try:
                        show_more = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, SEL_BTN_SHOW_MORE)))
                        driver.execute_script("arguments[0].click();", show_more)
                        print(f">>>>>>>>> Đã click 'Xem thêm' lần {click_count + 1}")
                        time.sleep(2); click_count += 1
                        current_products = len(driver.find_elements(By.CSS_SELECTOR, SEL_PRODUCT_CONTAINER))
                        if current_products >= MAX_PRODUCTS: break
                    except Exception:
                        print(">>>>>>>>> Hết nút 'Xem thêm' hoặc nút bị lỗi."); break
                print(f">>>>>>>>> Kết thúc vòng click.")

                soup = BeautifulSoup(driver.page_source, "html.parser")
                driver.quit(); print(">>>>>>>>> Đã đóng trình duyệt Chrome.")

                products = soup.select(SEL_PRODUCT_CONTAINER)
                all_products_data = [] 
                
                if not products:
                    print(f">>>>>>> KHÔNG TÌM THẤY sản phẩm nào với selector: {SEL_PRODUCT_CONTAINER}")
                    log_message(conn_control, SITE_ID, "CRAWL_WARN", rows=0, details_dict={"error": f"Không tìm thấy SP với selector: {SEL_PRODUCT_CONTAINER}"})
                    continue

                for item in products:
                    try:
                        name = item.select_one(SEL_NAME).get_text(strip=True) if SEL_NAME and item.select_one(SEL_NAME) else ""
                        link_tag = item.select_one(SEL_LINK)
                        link = link_tag['href'] if link_tag and link_tag.has_attr('href') else ""
                        img_tag = item.select_one(SEL_IMG)
                        img_link = ""
                        if img_tag: img_link = (img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-original") or img_tag.get("srcset") or "")
                        price_new = item.select_one(SEL_PRICE_NEW).get_text(strip=True) if SEL_PRICE_NEW and item.select_one(SEL_PRICE_NEW) else ""
                        price_old = item.select_one(SEL_PRICE_OLD).get_text(strip=True) if SEL_PRICE_OLD and item.select_one(SEL_PRICE_OLD) else ""
                        
                        size = ram = memory = ""
                        if SITE_NAME == "CELLPHONES":
                            badges = item.select(SEL_SCREEN) 
                            if badges:
                                if len(badges) > 0: size = badges[0].get_text(strip=True)
                                if len(badges) > 1: ram = badges[1].get_text(strip=True)
                                if len(badges) > 2: memory = badges[2].get_text(strip=True)
                        elif SITE_NAME == "TGDD": 
                            if SEL_SCREEN:
                                for b in item.select(SEL_SCREEN):
                                    t = b.get_text(strip=True)
                                    if '"' in t or "inch" in t.lower(): size = t; break
                            if SEL_RAM: 
                                mem_tag = item.select_one(SEL_RAM)
                                if mem_tag:
                                    parts = mem_tag.get_text(strip=True).split(" - ")
                                    if len(parts) == 2: ram, memory = parts
                                    elif len(parts) == 1:
                                        val = parts[0].upper().replace("GB", "").strip();
                                        try: num = int(val)
                                        except ValueError: num = 0
                                        if num >= 64: memory = parts[0].strip(); ram = ""
                                        else: ram = parts[0].strip(); memory = ""
                                        
                        smember_discount = item.select_one(SEL_DISCOUNT_SMEMBER).get_text(strip=True).replace("Smember giảm đến", "").strip() if SEL_DISCOUNT_SMEMBER and item.select_one(SEL_DISCOUNT_SMEMBER) else ""
                        sstudent_discount = item.select_one(SEL_DISCOUNT_STUDENT).get_text(strip=True).replace("S-Student giảm thêm", "").strip() if SEL_DISCOUNT_STUDENT and item.select_one(SEL_DISCOUNT_STUDENT) else ""
                        coupon_text = item.select_one(SEL_COUPON).get_text(strip=True) if SEL_COUPON and item.select_one(SEL_COUPON) else ""
                        discount_percent = item.select_one(SEL_DISCOUNT_PERCENT).get_text(strip=True) if SEL_DISCOUNT_PERCENT and item.select_one(SEL_DISCOUNT_PERCENT) else ""
                        gift_text = item.select_one(SEL_GIFT).get_text(strip=True) if SEL_GIFT and item.select_one(SEL_GIFT) else ""
                        rate_text = item.select_one(SEL_RATE).get_text(strip=True) if SEL_RATE and item.select_one(SEL_RATE) else ""
                        sold_text = item.select_one(SEL_SOLD_COUNT).get_text(strip=True) if SEL_SOLD_COUNT and item.select_one(SEL_SOLD_COUNT) else ""

                        if not name.strip(): continue
                            
                        if link and not link.startswith("http"):
                            if SITE_NAME == "TGDD": link = "https://www.thegioididong.com" + link
                            elif SITE_NAME == "CELLPHONES":
                                if link.startswith("/"): link = "https://cellphones.com.vn" + link

                        all_products_data.append({
                            'ID': product_counter_id, 'TEN': name, 'LINK': link, 'LINK_ANH': img_link, 'GIA_CU': price_old, 
                            'GIA_MOI': price_new, 'KICH_THUOC_MAN_HINH': size, 'RAM': ram, 'BO_NHO': memory,
                            'GIAM_GIA_SMEMBER': smember_discount, 'GIAM_GIA_SSTUDENT': sstudent_discount, 
                            'GIAM_GIA_PHAN_TRAM': discount_percent, 'COUPON': coupon_text, 'QUA_TANG': gift_text, 
                            'DANH_GIA': rate_text, 'DA_BAN': sold_text, 
                            'SITE_NAME': SITE_NAME, 'SITE_ID': SITE_ID
                        })
                        product_counter_id += 1
                        
                    except Exception as e:
                        print(f">>>>>>> Lỗi khi xử lý sản phẩm: {name} - {e}")
                        log_message(conn_control, SITE_ID, "PRODUCT_FAIL", rows=0, details_dict={"error": "Lỗi xử lý sản phẩm", "details": str(e), "product_name": name})
                
                writer.writerows(all_products_data)
                
                count = len(all_products_data)
                log_message(conn_control, SITE_ID, "CRAWL_SUCCESS", rows=count, 
                            details_dict={"file_path": CSV_FILE_PATH, "site": SITE_NAME})
                print(f">>>>>>>>> {SITE_NAME} - Đã lưu {count} sản phẩm vào file '{CSV_FILE_PATH}'.")

            except Exception as e:
                print(f"!!! LỖI NGHIÊM TRỌNG khi cào {SITE_NAME}: {e}")
                log_message(conn_control, SITE_ID, "CRAWL_FAIL", rows=0, details_dict={"error": f"Lỗi nghiêm trọng {SITE_NAME}", "details": str(e)})
                if 'driver' in locals(): driver.quit() 
                continue 
        
        print("\n--- HOÀN TẤT CÀO TẤT CẢ CÁC SITE ---")

except Exception as e:
    print(f"!!! LỖI KHI MỞ FILE CSV: {e}")
    log_message(conn_control, None, "CRAWL_FAIL", rows=0, details_dict={"error": "Lỗi nghiêm trọng khi mở/ghi file CSV", "details": str(e)})
finally:
    cursor_control.close()
    conn_control.close()
    print(">>>>>>>>> Đã đóng kết nối DB 'db_control'.")