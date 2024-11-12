import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from tqdm import tqdm
import logging
from datetime import datetime
from pathlib import Path

# 設定 logging 配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# 設定輸入和輸出資料夾路徑
# 請根據您的 Airflow 執行環境修改此路徑
INPUT_DIR = Path(__file__).parent.parent / 'raw_data_123' /'raw'   # 修改為適當的路徑
OUTPUT_DIR = Path(__file__).parent.parent / 'raw_data_123'/'after_1'  # 修改為適當的路徑

# 確保儲存資料夾存在
if not OUTPUT_DIR.exists(): # 確認是否有jobs_csv資料夾  沒有則返回Ture
    os.makedirs(OUTPUT_DIR) # 建立jobs_csv資料夾
    print('建立raw_data_123/after_1 資料夾完成')

# 獲取今天的日期，格式為 'YYYY-MM-DD'
date_today = datetime.today().strftime('%Y-%m-%d')

# 設置重試機制
session = requests.Session()
retry = Retry(connect=5, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

# 定義一個讀取網頁內容的函式，帶有隨機的 User-Agent 列表
def read_url(url):
    USER_AGENT_LIST = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        "Mozilla/5.0 (X11; U; Linux; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    ]
    USER_AGENT = random.choice(USER_AGENT_LIST)
    headers = {'User-Agent': USER_AGENT}

    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the page: {e}")
        return None

    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, "html.parser")
    return soup

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

# 大分類字典
category_dict = {
    "軟體工程": ['軟體工程師', 'Software Developer', '通訊軟體工程師', '韌體工程師', 'Firmware Engineer', '軟體測試人員', 'QA Engineer', 'BIOS工程師', 'BIOS Engineer', 'CIM工程師', 'MES工程師', '網站程式設計師', 'Web Developer'],
    "系統規劃": ['System Analyst', '系統分析師', 'System Engineer', '系統工程師'],
    "網路管理": ['MIS', '網路管理工程師'],
    "資料科學": ['數據科學家', '資料科學家', 'Data Scientist', '數據分析師', '資料分析師', 'Data Analyst', '數據架構師', 'Data Architect', '數據工程師', '資料工程師', 'Data Engineer', '機器學習工程師', 'Machine Learning Engineer', '演算法工程師', 'Algorithm engineer', '資料庫管理人員', 'DBA'],
    "雲服務/雲計算": ['雲端架構師', 'Cloud Architect', '雲端工程師', 'Cloud Engineer', '雲端資安', '雲端網路工程師', 'Network Engineer'],
    "資訊安全": ['資訊安全架構', '資安工程師', 'Cybersecurity Engineer', '資安滲透工程師'],
    "系統發展": ['前端工程師', 'Frontend Engineer', '後端工程師', 'Backend Engineer', '全端工程師', 'Full Stack Engineer', 'DevOps工程師', '區塊鏈工程師', 'Blockchain Engineer', '嵌入式工程師', 'Embedded Software Engineer', '自動化測試工程師', 'Automation QA Engineer', 'APP工程師']
}

# 小分類字典
subcategory_dict = {
    "軟體工程師": ['軟體工程師', 'Software Developer'],
    "通訊軟體工程師": ['通訊軟體工程師'],
    "韌體工程師": ['韌體工程師', 'Firmware Engineer'],
    "軟/韌體測試人員": ['軟體測試人員', 'QA Engineer'],
    "BIOS工程師": ['BIOS工程師', 'BIOS Engineer'],
    "CIM工程師": ['CIM工程師'],
    "MES工程師": ['MES工程師'],
    "網站程式設計師": ['網站程式設計師', 'Web Developer'],
    "系統分析師": ['System Analyst', '系統分析師'],
    "系統工程師": ['System Engineer', '系統工程師'],
    "網路管理工程師": ['MIS', '網路管理工程師'],
    "數據科學家": ['數據科學家', '資料科學家', 'Data Scientist'],
    "數據分析師": ['數據分析師', '資料分析師', 'Data Analyst'],
    "數據架構師": ['數據架構師', 'Data Architect'],
    "數據工程師": ['數據工程師', '資料工程師', 'Data Engineer'],
    "機器學習工程師": ['機器學習工程師', 'Machine Learning Engineer'],
    "演算法工程師": ['演算法工程師', 'Algorithm engineer'],
    "資料庫管理人員": ['資料庫管理人員', 'DBA'],
    "雲端架構師": ['雲端架構師', 'Cloud Architect'],
    "雲端工程師": ['雲端工程師', 'Cloud Engineer'],
    "雲端資安": ['雲端資安'],
    "雲端網路工程師": ['雲端網路工程師', 'Network Engineer'],
    "資訊安全架構": ['資訊安全架構'],
    "資安工程師": ['資安工程師', 'Cybersecurity Engineer'],
    "資安滲透工程師": ['資安滲透工程師'],
    "前端工程師": ['前端工程師', 'Frontend Engineer'],
    "後端工程師": ['後端工程師', 'Backend Engineer'],
    "全端工程師": ['全端工程師', 'Full Stack Engineer'],
    "DevOps工程師": ['DevOps工程師'],
    "區塊鏈工程師": ['區塊鏈工程師', 'Blockchain Engineer'],
    "嵌入式工程師": ['嵌入式工程師', 'Embedded Software Engineer'],
    "自動化測試工程師": ['自動化測試工程師', 'Automation QA Engineer'],
    "APP工程師": ['APP工程師']
}

def scrape_job_details():
    # 使用 tqdm 來顯示進度條，遍歷關鍵字列表
    for keyword in tqdm(keywords_list, desc="Processing keywords"):
        # 動態獲取日期，並生成輸入和輸出 CSV 檔案路徑
        csv_file = os.path.join(INPUT_DIR, f"{keyword}_{date_today}.csv")

        if not os.path.exists(csv_file):
            logging.warning(f"檔案 {csv_file} 不存在，跳過...")
            continue

        try:
            data = pd.read_csv(csv_file)
        except Exception as e:
            logging.error(f"讀取檔案 {csv_file} 時出錯: {e}，跳過...")
            continue

        # 取得檔案名稱（不帶副檔名）
        file_name = os.path.splitext(os.path.basename(csv_file))[0]
        # 將檔案名後面加上 _info
        output_csv_file = f"{file_name}_info.csv"
        output_csv_path = os.path.join(OUTPUT_DIR, output_csv_file)

        results = []

        # 使用 tqdm 來顯示每個關鍵字下的處理進度
        for index, row in tqdm(data.iterrows(), total=data.shape[0], desc=f"Processing {keyword}", leave=False):
            if pd.isnull(row['連結']):
                logging.info(f"索引 {index} 的列沒有 '連結' 值，跳過...")
                continue

            url = row['連結'].strip()
            soup = read_url(url)
            if soup is None:
                continue

            # 職位名稱、公司名稱
            job_title = soup.find('h1', id='limit_word_count').text.strip() if soup.find('h1', id='limit_word_count') else pd.NA
            company_name = soup.find('div', class_='box_firm_name').a.text.strip() if soup.find('div', class_='box_firm_name') else pd.NA

            # 徵才說明、工作地點
            job_description_section = soup.find('div', class_='job_explain mt')
            job_details = job_description_section.find_all('li') if job_description_section else []
            job_content = job_details[0].find('span', class_='right_main').text.strip() if len(job_details) > 0 and job_details[0].find('span', class_='right_main') else pd.NA

            # 工作地點
            job_location = pd.NA
            for li in job_details:
                left_title = li.find('span', class_='left_title')
                if left_title and left_title.text.strip() == "工作地點：":
                    job_location_element = li.find('span', class_='right_main').find('a')
                    job_location = job_location_element.text.strip() if job_location_element else (li.find('span', class_='right_main').text.strip() if li.find('span', class_='right_main') else pd.NA)
                    break

            # 工作條件
            job_conditions_section = soup.find('div', id='a2')
            job_conditions_section = job_conditions_section.find_next('ul') if job_conditions_section else None
            job_conditions = job_conditions_section.find_all('li') if job_conditions_section else []
            education = job_conditions[0].find('span', class_='right_main').text.strip() if len(job_conditions) > 0 and job_conditions[0].find('span', class_='right_main') else pd.NA
            field_of_study = job_conditions[1].find('span', class_='right_main').text.strip() if len(job_conditions) > 1 and job_conditions[1].find('span', class_='right_main') else pd.NA
            experience = job_conditions[2].find('span', class_='right_main').text.strip() if len(job_conditions) > 2 and job_conditions[2].find('span', class_='right_main') else pd.NA

            # 技能與求職專長
            computer_skills = pd.NA
            skills_section = soup.find_all('div', class_='job_explain')
            for section in skills_section:
                h3_tag = section.find('h3')
                if h3_tag and "技能與求職專長" in h3_tag.text:
                    right_main_span = section.find('span', class_='right_main')
                    computer_skills = right_main_span.text.strip() if right_main_span else pd.NA
                    break

            # 其他條件
            other_conditions = pd.NA
            for section in skills_section:
                h3_tag = section.find('h3')
                if h3_tag and "其他條件" in h3_tag.text:
                    exception_span = section.find('span', class_='exception')
                    other_conditions = exception_span.text.strip() if exception_span else pd.NA
                    break

            # 職務類別
            job_category = pd.NA
            for li in job_details:
                left_title = li.find('span', class_='left_title')
                if left_title and left_title.text.strip() == "職務類別：":
                    job_category_element = li.find('span', class_='right_main').find('a')
                    job_category = job_category_element.text.strip() if job_category_element else pd.NA
                    break

            # 薪資待遇
            salary = pd.NA
            for li in job_details:
                left_title = li.find('span', class_='left_title')
                if left_title and left_title.text.strip() == "薪資待遇：":
                    salary_element = li.find('span', class_='right_main')
                    if salary_element:
                        salary_text = salary_element.text.strip()

                        # 1. 包含 "薪資面議" 時，顯示為 NaN
                        if "薪資面議" in salary_text:
                            salary = pd.NA

                        # 3. 遇到 "年薪" 時，將數字除以 13 後顯示
                        elif "年薪" in salary_text:
                            # 提取年薪數字
                            year_salary_part = salary_text.split("年薪")[1]
                            year_salary_number = ''.join(filter(str.isdigit, year_salary_part.split("至")[0].strip()))
                            if year_salary_number:
                                salary = int(year_salary_number) // 13
                            else:
                                salary = pd.NA

                        # 2. 有區間的部分，只取 "至" 前面的數字
                        elif "至" in salary_text:
                            salary_number = ''.join(filter(str.isdigit, salary_text.split("至")[0].strip()))
                            salary = int(salary_number) if salary_number else pd.NA

                        # 無其他特別情況，僅保留數字部分
                        else:
                            salary_number = ''.join(filter(str.isdigit, salary_text))
                            salary = int(salary_number) if salary_number else pd.NA
                    break

            # 來源、大分類、小分類
            source = "Yes123"
            main_category = next((main_cat for main_cat, keywords in category_dict.items() if keyword in keywords), pd.NA)
            sub_category = next((sub_cat for sub_cat, sub_keywords in subcategory_dict.items() if keyword in sub_keywords), pd.NA)

            # 欄目列表
            results.append({
                "日期": row['日期'] if not pd.isnull(row['日期']) else pd.NA,
                "職位名稱": job_title,
                "公司名稱": company_name,
                "工作內容": job_content,
                "工作地點": job_location,
                "學歷要求": education,
                "科系要求": field_of_study,
                "工作經驗": experience,
                "電腦技能": computer_skills,
                "其他條件": other_conditions,
                "職務類別": job_category,
                "薪資待遇": salary,
                "來源": source,
                "大分類": main_category,
                "小分類": sub_category,
                "產業別": pd.NA,
                "連結": url,
                "地區": "台灣"
            })

        # 將結果存儲為CSV，檔名為原檔名加上 "_info.csv"
        if results:
            output_df = pd.DataFrame(results)
            output_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
            logging.info(f"已儲存結果到 {output_csv_path}")
        else:
            logging.info(f"關鍵字 {keyword} 沒有任何結果被儲存。")

def main():
    scrape_job_details()

if __name__ == "__main__":
    main()
