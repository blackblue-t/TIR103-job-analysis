import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import pandas as pd
import numpy as np
import re

keys = ['軟體工程師', 'Software Developer', '通訊軟體工程師', '韌體工程師', 'Firmware Engineer',
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
    '自動化測試工程師', 'Automation QA Engineer', 'APP工程師']



url = "https://www.cake.me/jobs"
all_company_links = {key: [] for key in keys}  # 使用字典來存儲每個關鍵字的鏈接

def fetch_links(key, page_range):
    for i in range(page_range[0], page_range[1]):
        page_url = f"{url}/{key}?page={i}"
        try:
            response = requests.get(page_url)
            response.raise_for_status()  # 檢查請求是否成功
            
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all('a', class_='JobSearchItem_jobTitle__bu6yO')
            for link in links:
                href = link.get('href')
                if href and href not in all_company_links[key]:  # 確保 href 存在且不重複
                    # 將前綴加到 href
                    full_url = f"https://www.cake.me{href}" if not href.startswith("http") else href
                    all_company_links[key].append(full_url)  # 將完整 URL 添加到對應的關鍵字列表
        except requests.exceptions.RequestException as e:
            
            break  # 跳出當前頁面循環，開始下一個關鍵字
        
        time.sleep(5)  # 暫停5秒

# 遍歷所有關鍵字
for key in keys:
    for start in range(1, 101, 20):
        fetch_links(key, (start, start + 20))

def get_job_data(url, key):  # 增加 key 參數
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # 提取所需的資料
    job_title_elements = soup.find('h1', class_='JobDescriptionLeftColumn_title__4MYvd')
    job_title = job_title_elements.get_text(strip=True) if job_title_elements else "N/A"
    
    company_name_elements = soup.find('a', class_='JobDescriptionLeftColumn_name__ABAp9')
    company_name = company_name_elements.get_text(strip=True) if company_name_elements else "N/A"
    
    job_description_elements = soup.find('div', class_='JobDescriptionLeftColumn_row__iY44x JobDescriptionLeftColumn_mainContent__VrTGs')
    job_description = job_description_elements.get_text(strip=True) if job_description_elements else "N/A"
    
    location_element = soup.find('a', class_='CompanyInfoItem_link__cMZwX')
    location = location_element.get_text(strip=True) if location_element else "N/A"

    years_of_experience = soup.select('div.JobDescriptionRightColumn_jobInfo__9Liba > div:nth-child(4) > span')
    years_of_experience = years_of_experience[0].text if years_of_experience else "N/A"

    salary = soup.select('div.JobDescriptionRightColumn_jobInfo__9Liba > div:nth-child(5) > div.JobDescriptionRightColumn_salaryWrapper__Q_8IL > span')
    salary = salary[0].text if salary else "N/A"

    company_props = soup.select('div > div.LayoutJob_container__JSBvV > div.AboutBlock_container__LSgHt > div.AboutBlock_textContent__5UUQl > div.AboutBlock_headerRow__WZhTX > div.AboutBlock_companyMeta__O1aEE > div > div:nth-child(1) > span')
    company_props = company_props[0].text if company_props else "N/A"


    # 整合資料，新增 'key'
    job_data = {
        "category_secondary": key,  # 新增關鍵字
        "job_title": job_title,
        "company_name": company_name,
        "industry": company_props,
        "description": job_description,
        "location_region": location,
        "experience": years_of_experience,
        "salary": salary,
        "report_date": datetime.now().strftime("%Y-%m-%d"),
        "url": url
    }
    return job_data  

# 把所有 URL 跑一遍然後把資料做成 DataFrame
results = []
for key, links in all_company_links.items():  # 迭代字典中的每個關鍵字和對應的鏈接
    for link in links:
        datas = get_job_data(link, key)  # 傳遞關鍵字
        results.append(datas)
        time.sleep(4)  # 暫停 4 秒以避免過於頻繁的請求

# 創建 DataFrame
df = pd.DataFrame(results)

# 用來調整salary欄位的function

# 篩選台幣
def check_salary(value):
    # 如果是空值，直接返回
    if pd.isna(value):
        return np.nan
    
    # 檢查是否包含 'TWD', 'twd', 'Twd'
    if 'TWD' in str(value) or 'twd' in str(value) or 'Twd' in str(value):
        return value  # 如果包含，保留原值
    else:
        return np.nan  # 否則返回空值

# 對欄位用函數
df['salary'] = df['salary'].apply(check_salary)
# 逗號去除
df['salary'] = df['salary'].str.replace(',', '', regex=False)

# 處理薪資
def convert_salary(salary):
    # 如果是空值，直接返回
    if pd.isna(salary):
        return np.nan

    # 將薪資轉為字符串以便處理
    salary_str = str(salary).strip()
    
    # 檢查單一數字情況（例如，1500000 TWD 或 38000+ TWD）
    single_value_match = re.match(r'(\d+)\+?\s*TWD', salary_str)
    if single_value_match:
        # 提取數字並計算月薪，這裡假設它是年薪
        return int(single_value_match.group(1)) / 12

    # 檢查年薪格式
    yearly_match = re.search(r'(\d+)\+?\s*~?\s*(\d+)?\s*TWD\s*/\s*year', salary_str)
    if yearly_match:
        # 提取低的薪資
        low_salary = int(yearly_match.group(1))
        # 計算月薪
        monthly_salary = low_salary / 12
        return monthly_salary
    
    # 檢查月薪格式
    monthly_match = re.search(r'(\d+)\+?\s*~?\s*(\d+)?\s*TWD\s*/\s*month', salary_str)
    if monthly_match:
        # 直接取第一個數字
        return int(monthly_match.group(1))

    # 檢查時薪格式
    hourly_match = re.search(r'(\d+)\+?\s*~?\s*(\d+)?\s*TWD\s*/\s*hour', salary_str)
    if hourly_match:
        # 取第一個數字作為時薪
        hourly_wage = int(hourly_match.group(1))
        # 計算月薪：假設每天工作8.5小時，每月工作24天
        monthly_salary = hourly_wage * 8.5 * 24
        return monthly_salary
    
    # 如果格式不符合，返回空值
    return np.nan

# 對欄位應用函數
df['salary'] = df['salary'].apply(convert_salary)
# 將大於270000或小於25000的數據替換為空值，因為這些數值怪怪的
df['salary'] = df['salary'].apply(lambda x: np.nan if x > 270000 or x < 25000 else x)

# 用來調整location_region的function

# 自定義函數來處理欄位值
def process_location(value):
    # 如果是空值，直接返回
    if pd.isna(value):
        return np.nan

    # 去掉「台灣」或「臺灣」
    value = re.sub(r'台灣|臺灣|taiwan|Taiwan|TAIWAN', '', str(value))
    
    # 如果開頭是數字，刪除數字部分
    value = re.sub(r'^\d+', '', value).strip()
    
    # 刪除「園區」這個字
    value = re.sub(r'園區', '', value)

    # 如果不包含「區」，返回空值
    if '區' not in value:
        return np.nan
    
    # 保留第一個「區」前的部分，包括「區」字
    match = re.search(r'^(.*?區)', value)
    if match:
        return match.group(1)  # 只保留「區」前的內容（包含「區」）

    return np.nan  # 如果沒有符合的內容，返回空值

# 對欄位應用函數
df['location_region'] = df['location_region'].apply(process_location).apply(lambda x: x.lstrip(',') if isinstance(x, str) else x)

# industry欄位的錯誤值去掉
# 定義要替換的數字（作為字串）
numbers_to_replace = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'}

# 將包含特定數字的欄位值替換為空值
df['industry'] = df['industry'].apply(lambda x: np.nan if any(num in str(x) for num in numbers_to_replace) else x)

# 處理description使得一段雜亂的text可以擷取其中的關鍵字

# 去除表情符號或是一些圖案
def clean_description(description):
    # 使用正則表達式去除非中文、英文、數字的字符
    cleaned = re.sub(r'[^A-Za-z0-9\u4e00-\u9fa5]+', ' ', description)  # 替換為單個空格
    cleaned = cleaned.strip()  # 去掉前後空格
    return cleaned

df['description'] = df['description'].apply(clean_description)

keywords = [
    "python", "c#", ".net", "etl", "openmetadata", "azure", "teradata", "ms", "sql", "pipeline",
    "unix", "kubernetes", "mysql", "hive", "sqlite", "powerbi", "redshift", "guardium", "jenkins",
    "javascript", "cdp", "pentaho", "gcp", "ga4", "django", "pandas", "mariadb", "ios", "protocol",
    "framework", "gitlab", "fastapi", "java", "scrum", "uikit", "arkit", "dataflow", "vba", "aws",
    "jquery", "numpy", "microsoft", "datalake", "tableau", "airflow", "csv", "excel", "mssql",
    "html5", "css", "vue", "svg", "seo", "rwd", "pwa", "linux", "hadoop", "docker", "mongodb",
    "redis", "golang", "nlp", "kafka", "c++", "apache", "oracle", "c", "bigquery", "pytorch", "nas",
    "react", "schema", "flink", "vb", "nosql", "stack", "tensorflow", "rust", "typescript", "ansible",
    "terraform", "elasticsearch", "selenium", "f#", "graphql", "swift", "perl", "clojure", "r", 
    "matlab", "scala", "dart", "lua", "haskell", "objective-c", "cobol", "fortran", "julia", 
    "erlang", "forth", "vhdl", "prolog", "ada", "tcl", "assembly", "scheme", "ocaml", "crystal", 
    "elixir", "zig", "nim", "vala", "d", "simula", "racket", "smalltalk", "sml", "vbscript", 
    "groovy", "verilog", "ballerina", "coldfusion", "powershell", "hack", "zsh", "fish", 
    "bash", "kornshell", "makefile", "pl/sql", "transact-sql", "lisp", "f#", "sas", "awk", 
    "sed", "rpg", "turing", "algol"]

# 提取關鍵字的function
def extract_tools(description):
    # 將描述轉為小寫以便於匹配
    description_lower = description.lower()
    # 在描述中查找關鍵字並收集
    found_tools = [keyword for keyword in keywords if keyword in description_lower]
    return ', '.join(found_tools) if found_tools else "N/A"  # 若找到工具則返回，否則返回 N/A

df['description'] = df['description'].apply(extract_tools)
df = df.rename(columns={'description': 'tools'})
df['tools'] = df['tools'].str.split(',')
df = df.explode('tools').reset_index(drop=True)

category_dict = {
    "軟體工程": [
        "軟體工程師", "Software Developer", "通訊軟體工程師", "韌體工程師", "Firmware Engineer",
        "軟體測試人員", "QA Engineer", "BIOS工程師", "BIOS Engineer", "CIM工程師",
        "MES工程師", "網站程式設計師", "Web Developer"
    ],
    "系統規劃": [
        "System Analyst", "系統分析師", "System Engineer", "系統工程師"
    ],
    "網路管理": [
        "MIS", "網路管理工程師"
    ],
    "資料科學": [
        "數據科學家", "資料科學家", "Data Scientist", "數據分析師", "資料分析師", "Data Analyst",
        "數據架構師", "Data Architect", "數據工程師", "資料工程師", "Data Engineer",
        "機器學習工程師", "Machine Learning Engineer", "演算法工程師", "Algorithm engineer",
        "資料庫管理人員", "DBA"
    ],
    "雲服務/雲計算": [
        "雲端架構師", "Cloud Architect", "雲端工程師", "Cloud Engineer", "雲端資安",
        "雲端網路工程師", "Network Engineer"
    ],
    "資訊安全": [
        "資訊安全架構", "資安工程師", "Cybersecurity Engineer", "資安滲透工程師"
    ],
    "系統發展": [
        "前端工程師", "Frontend Engineer", "後端工程師", "Backend Engineer", "全端工程師",
        "Full Stack Engineer", "DevOps工程師", "區塊鏈工程師", "Blockchain Engineer",
        "嵌入式工程師", "Embedded Software Engineer", "自動化測試工程師",
        "Automation QA Engineer", "APP工程師"
    ]
}

def get_category_first(title):
    for key, values in category_dict.items():
        if title in values:  # 檢查 title 是否在 values 列表中
            return key
    return None  # 如果沒有找到，返回 None

# 使用 apply 方法和自定義函數來創建新的欄位
df['category_primary'] = df['category_secondary'].apply(get_category_first)
df.to_csv('1107data_part.csv',encoding='utf-8')
