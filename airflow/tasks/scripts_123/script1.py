import requests
from bs4 import BeautifulSoup
import random
import time
from urllib.parse import quote
import csv
import re
from datetime import date
import os
from tqdm import tqdm
from pathlib import Path

# 設定儲存資料夾路徑
# 您可以根據需求修改此路徑，確保 Airflow 的執行環境有權限寫入
SAVE_FOLDER = Path("/opt/airflow/tasks/raw_data_123/raw")

# 確保儲存資料夾存在
if not SAVE_FOLDER.exists(): # 確認是否有jobs_csv資料夾  沒有則返回Ture
    os.makedirs(SAVE_FOLDER) # 建立jobs_csv資料夾
    print('建立raw_data_123/raw 資料夾完成')

# 取得今天的日期
def get_todate():
    return date.today()

# 讀取網頁內容並解析
def read_url(url):
    USER_AGENT_LIST = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko) Safari/419.3 Arora/0.6",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    ]
    headers = {'user-agent': random.choice(USER_AGENT_LIST)}
    try:
        req = requests.get(url, headers=headers)
        req.encoding = 'utf-8'
        return BeautifulSoup(req.text, "html.parser")
    except requests.RequestException as e:
        print(f"請求 URL {url} 時發生錯誤: {e}")
        return None

# 取得總頁數
def get_total_pages(soup):
    try:
        select_element = soup.select_one('select#inputState')
        if select_element is None:
            print("無法找到頁數選擇器，使用預設最大頁數 5")
            return 5  # 無法找到時使用預設最大頁數
        options = select_element.find_all('option')
        return int(options[-1].get('value'))  # 獲取最後一個 <option> 的 value 作為總頁數
    except Exception as e:
        print(f"無法解析總頁數: {e}")
        return 5  # 預設最大頁數

# 儲存過濾後的資料到 CSV 檔案
def save_filtered_to_csv(all_filtered_data):
    for keyword, data in all_filtered_data.items():
        if data:  # 確保有數據
            filtered_path_csv = SAVE_FOLDER / f'{keyword}_{get_todate()}.csv'
            try:
                with open(filtered_path_csv, mode='w', newline='', encoding='utf-8') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(['日期', '工作名稱', '公司名稱', '地點', '連結'])
                    csvwriter.writerows(data)
                print(f"已儲存 {keyword} 的結果到 {filtered_path_csv}")
            except Exception as e:
                print(f"儲存 {keyword} 的結果時發生錯誤: {e}")
        else:
            print(f"沒有找到符合關鍵字 {keyword} 的職缺")

# 地區代碼的字典
zone_codes = {
    '基隆全區': '1_1001_0001_0000',
    '新北市全區': '1_1001_0002_0000',
    '台北市全區': '1_1001_0003_0000',
    '桃園全區': '1_1001_0004_0000',
    '新竹縣全區': '1_1001_0005_0000',
    '新竹市全區': '1_1001_0006_0000',
    '苗栗全區': '1_1001_0007_0000',
    '台中市全區': '1_1001_0008_0000',
    '彰化全區': '1_1001_0010_0000',
    '南投全區': '1_1001_0011_0000',
    '雲林全區': '1_1001_0012_0000',
    '嘉義市全區': '1_1001_0013_0000',
    '嘉義縣全區': '1_1001_0014_0000',
    '台南市全區': '1_1001_0016_0000',
    '高雄市全區': '1_1001_0017_0000',
    '屏東全區': '1_1001_0019_0000',
    '宜蘭全區': '1_1001_0020_0000',
    '花蓮全區': '1_1001_0021_0000',
    '台東全區': '1_1001_0022_0000'
}

# 關鍵字列表
keywords_list = [
    '軟體工程師', 'Software Developer', '通訊軟體工程師', '韌體工程師', 'Firmware Engineer',
    '軟體測試人員', 'QA Engineer', 'BIOS工程師', 'BIOS Engineer', 'CIM工程師',
    'MES工程師', '網站程式設計師', 'Web Developer', 'System Analyst', '系統分析師',
    'System Engineer', '系統工程師', 'MIS', '網路管理工程師', '數據科學家', '資料科學家',
    'Data Scientist', '數據分析師','資料分析師', 'Data Analyst', '數據架構師', 'Data Architect',
    '數據工程師', '資料工程師', 'Data Engineer', '機器學習工程師', 'Machine Learning Engineer',
    '演算法工程師', 'Algorithm engineer', '資料庫管理人員', 'DBA', '雲端架構師',
    'Cloud Architect', '雲端工程師', 'Cloud Engineer', '雲端資安', '雲端網路工程師',
    'Network Engineer', '資訊安全架構', '資安工程師', 'Cybersecurity Engineer',
    '資安滲透工程師', '前端工程師', 'Frontend Engineer', '後端工程師',
    'Backend Engineer', '全端工程師', 'Full Stack Engineer', 'DevOps工程師',
    '區塊鏈工程師', 'Blockchain Engineer', '嵌入式工程師', 'Embedded Software Engineer',
    '自動化測試工程師', 'Automation QA Engineer', 'APP工程師'
]

# 儲存所有結果的字典
all_filtered_data = {keyword: [] for keyword in keywords_list}

# 主爬取功能
def find_title_123(keyword, zone, all_filtered_data):
    key = quote(keyword)
    strrec = 0
    seen_jobs = set()  # 儲存已處理的工作連結，避免重複
    find_zone = zone_codes.get(zone, 'N/A')  # 根據地區名稱查找對應的代碼

    base_url = f'https://www.yes123.com.tw/wk_index/joblist.asp?find_key2={key}&find_zone_mode1={find_zone}&s_find_zone_mode1=&order_ascend=desc&search_key_word={key}&search_from=joblist'
    soup = read_url(f"{base_url}&strrec=0")
    if soup is None:
        print(f"無法讀取初始頁面，跳過關鍵字 {keyword} 和地區 {zone}")
        return
    total_pages = get_total_pages(soup)  # 解析總頁數

    # 迴圈爬取每一頁的數據
    while strrec <= (total_pages - 1) * 30:
        url = f'{base_url}&strrec={strrec}'
        try:
            soup = read_url(url)
            if soup is None:
                print(f"無法讀取頁面 {strrec // 30 + 1}，跳過...")
                break
        except Exception as e:
            print(f'讀取 URL {url} 時發生錯誤: {e}')
            break  # 無法讀取頁面時跳出迴圈

        # 增加延遲時間以避免被封鎖
        time.sleep(random.randint(3, 6))  # 增加延遲時間

        job_items = soup.select('.Job_opening')
        if not job_items:
            print(f"頁面 {strrec // 30 + 1} 沒有找到工作項目，跳過...")
            break

        # 檢查這一頁是否有任何符合關鍵字的職缺
        found_any_match = False

        # 解析每一個工作項目
        for job_item in job_items:
            try:
                title_element = job_item.select('h5 a')
                if not title_element:
                    continue
                title = title_element[0].get_text().strip()  # 正確提取標題
                company_name = job_item.select('h6 a')[0].get_text().strip()
                location = job_item.select('.Job_opening_item_info span')[0].get_text().strip()
                job_link = job_item.select('h5 a')[0]['href']
                full_url = f'https://www.yes123.com.tw/wk_index/{job_link}'

                # 檢查工作是否重複
                if full_url in seen_jobs:
                    continue
                seen_jobs.add(full_url)

                # 檢查這個工作是否符合關鍵字
                if keyword.lower() in title.lower():  # 忽略大小寫來比對
                    all_filtered_data[keyword].append([get_todate(), title, company_name, location, full_url])
                    found_any_match = True  # 標記為找到符合關鍵字的工作
            except Exception as e:
                print(f'解析工作時發生錯誤: {e}')
                continue

        # 如果這兩頁內沒有符合關鍵字的工作，結束這次搜尋，跳到下一個關鍵字
        if not found_any_match:
            print(f'關鍵字 "{keyword}" 在頁面 {strrec // 30 + 2} 沒有找到符合的職缺，跳到下一個關鍵字...')
            break

        strrec += 30  # 翻到下一頁

def main():
    # 執行爬取功能，根據每個地區進行搜尋，並使用 tqdm 顯示進度條
    for zone in tqdm(zone_codes, desc='地區'):
        for keyword in tqdm(keywords_list, desc='關鍵字', leave=False):
            try:
                print(f'開始爬取地區：{zone} 關鍵字：{keyword}')
                find_title_123(keyword, zone, all_filtered_data)
            except Exception as e:
                print(f'處理地區 {zone} 關鍵字 {keyword} 時發生錯誤: {e}')

    # 儲存所有關鍵字的結果到 SAVE_FOLDER
    save_filtered_to_csv(all_filtered_data)

if __name__ == "__main__":
    main()