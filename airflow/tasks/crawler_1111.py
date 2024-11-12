import requests
from bs4 import BeautifulSoup
import re
import random
import time
from urllib.parse import quote
from tqdm import tqdm, trange
import csv

import pandas as pd
from datetime import date
import os,os.path
from pathlib import Path

"""#1111人力銀行 爬蟲"""

def get_todate(): # 取檔名用的
    return date.today()



def read_url(url):
    USER_AGENT_LIST = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        ]
    USER_AGENT = random.choice(USER_AGENT_LIST)

    headers = {'User-Agent': USER_AGENT}
    s = requests.Session()
    req = s.get(url, headers = headers)
    time.sleep(random.randint(3,6))
    req.raise_for_status()
    req.encoding = 'utf-8'
    soup = BeautifulSoup(req.text, "html.parser")
    return soup



############ 1111人力銀行 #########


def get_page_Code_1111(url):
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    jobsum = soup.select_one('div.top > div.left > p >span').text.strip()

    page =(int(jobsum)//20)+1
    #一頁約20個工作
    # page = page.split('/')
    # page = page[1].strip(' ')
    if page > 150:
      page = 150
    #暫時找不到總頁數的位置，以最大頁數下搜尋
    return page


def csv_column_1111(path_csv): #建立行標題
    csv_path = path_csv.with_suffix('.csv')
    with open(csv_path, mode='a+', newline='', encoding='utf-8') as employee_file:
        employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        employee_writer.writerow(['日期', '工作名稱', '公司名稱', '公司地址', '薪資', '工作內容(簡)', '地區', '經歷', '學歷', '工作網址',"工作內容(詳細)","職務類別","工作技能","電腦專長","附加條件","公司產業類別"])

def find_data_1111(soup):
    #錢錢(最低)
    mnone =[]
    spans = soup.select('div.other>span')
    for span in spans:
      # 提取 data-after 屬性
      data_after = span.get('data-after')

      # 使用正則表達式來提取數字部分
      match = re.search(r'(\d{1,3}(,\d{3})*)', data_after)
      #有不支薪的義工工作
      if match  == None:
        salary = '無'
      else:
        salary = match.group(1)

      #面議（經常性薪資4萬/月含以上）直接給40000

      if salary == '4':
          salary = '40,000'
    # 將數字加入到 mnone中
      mnone.append(salary)
    #要小心月薪時薪年薪

    #縣市區域
    location = []
    spans = soup.select('div.other > a')
    for span in spans:

      data_after = span.get('data-after')
      location.append(data_after)

    #日期
    get_date =[]
    spans = soup.select('div.buttom.d-block > div > p')
    for span in spans:

      get_date.append(span.text)

    #簡介
    jbInfoTxt = []
    spans = soup.select('div.main_content > p')
    for span in spans:

      jbInfoTxt.append(span.text)

    #網址
    jobs_url = []
    spans = soup.select('div.title.position0 > a')
    for span in spans:
      jobs_url.append(span['href'])

    #公司名稱、類別、住址
    company_data = []
    spans = soup.select('div.company.organ > a')
    for span in spans:
      company_data.append(span['title'])
    #工作標題
    title = []
    spans = soup.select('div.title.position0 > a')
    for span in spans:
      title.append(span.text)
    #工作經驗
    jobs_exp =[]
    spans = soup.select('div > span.experience.d-md-block.d-none')
    for span in spans:
      data_before = span.get('data-before')
      jobs_exp.append(data_before)

    # 學歷
    education = []
    spans = soup.select('div > span.educational.d-md-block.d-none')
    for span in spans:
      data_before = span.get('data-before')
      education.append(data_before)
    #更多內容
    #"工作內容(詳細)"
    jobdetail = []
    #"職務類別"
    jobclass = []
    #"工作技能"
    job_skill = []
    #"電腦專長"
    computer_skill = []
    #"附加條件"
    ad_condition = []
    #"公司產業"
    industry = []

    for web_url in jobs_url:
      detail_url = 'https://www.1111.com.tw'+web_url

      detail_soup = read_url(detail_url)
      #工作被下架會出現工作已下架
      if detail_soup.select('.card_content_wrapper') == []:
        print(f"此工作已結束徵才{detail_url}")
        #防止格式跑掉
        jobdetail.append("工作已結束徵才")
        jobclass.append("工作已結束徵才")
        job_skill.append("工作已結束徵才")
        computer_skill.append("工作已結束徵才")
        ad_condition.append("工作已結束徵才")
        continue

      #"工作內容(詳細)"

      jobdetail.append(detail_soup.select('.body_2.description_info')[0].get_text().strip().replace(" ",""))

      #"職務類別"
      joblist=""
      get_all = detail_soup.select('.dropdown-toggle')
      for job in get_all[3:]:
        joblist = joblist+","+job.text.strip()

      jobclass.append(joblist)

      #"工作技能"
      skilllist=""
      get_all = detail_soup.select('.secondary-tag-nounderline')
      for get in get_all:
        skilllist = skilllist+","+get.text.strip()
      job_skill.append(skilllist)

      #"電腦專長"
      computerskill = ""
      get_all = detail_soup.select('.btn_secondary_5.btn_size_5.mr-2.mb-2')
      for get in get_all:
        computerskill = computerskill+","+get.text.strip()
      computer_skill.append(computerskill)

      #"附加條件"
      #有時候如果語言能力沒有要求，要修改抓取的位置

      try:
        ad_condition.append(detail_soup.select_one('.d-flex.m_info_group.conditions').select_one('.ui_items_group').text.strip())
      except:
        ad_condition.append("無")
      #"公司產業"
      try:
        industry.append(detail_soup.select_one('.ui_card_company_link').select_one('.body_3').text.strip())
      except:
        industry.append("無")

    return mnone, location, jbInfoTxt, jobs_url, company_data, title, jobs_exp, get_date, education, jobdetail, jobclass, job_skill, computer_skill, ad_condition, industry



def find_title_1111(key_txt):
    #路徑組合
    today = get_todate()
    base_path = Path(__file__).parent.parent / 'tasks'/'raw_data_1111'
    path_csv = base_path /  f"{today}{key_txt}_1111人力銀行_TBD.csv"
    if not base_path.exists(): # 確認是否有jobs_csv資料夾  沒有則返回Ture
        os.makedirs(base_path ) # 建立jobs_csv資料夾
        print('建立raw_data_1111 資料夾完成')
    csv_column_1111(path_csv) #建立行標題
    key = quote(key_txt)
    find_page_url = 'https://www.1111.com.tw/search/job?si=1&ss=s&ks={0}&page=1'.format(key)
    #取得最大page數
    get_sum_page = int(get_page_Code_1111(find_page_url))
    print('共有：' + str(get_sum_page) + ' 頁')


    for i in tqdm(range(1, get_sum_page+1)):
      # tqdm是進度條的套件
        url = 'https://www.1111.com.tw/search/job?ks={0}&page={1}&si=1'.format(key, i)
        soup = read_url(url) #讀取網頁

        #讀取網頁資料
        mnone, location, jbInfoTxt, jobs_url, company_data, title, jobs_exp, get_date, education, jobdetail, jobclass, job_skill, computer_skill, ad_condition, industry = find_data_1111(soup)
        print('目前爬取頁面是：' + url)
        for mnone, location, jbInfoTxt, jobs_url, company_data, title, jobs_exp, get_date, education, jobdetail, jobclass, job_skill, computer_skill, ad_condition, industry in zip(mnone, location, jbInfoTxt, jobs_url, company_data, title, jobs_exp, get_date, education, jobdetail, jobclass, job_skill, computer_skill, ad_condition, industry):
            #錢 取最低薪資
            get_mone = mnone
            #日期
            get_date = get_date

            #縣市區域
            location = location

            #簡介
            jbInfoTxt = jbInfoTxt.replace("\xa0", "") #刪除\xa0

            #工作網址
            jobs_url = 'https://www.1111.com.tw{0}'.format(jobs_url)

            #公司名
            company = company_data.split('\n')[0][6:]

            #公司分類 目前暫不使用
            category = company_data.split('\n')[1][6:]

            #公司地址
            address = company_data.split('\n')[2][6:]

            #工作標題
            title = title

            # 工作經驗
            jobs_exp = jobs_exp

            # 學歷
            education = education

            #詳細工作內容
            #"工作內容(詳細)"
            jobdetail = jobdetail

            #"職務類別"
            jobclass = jobclass

            #"工作技能"
            job_skill = job_skill

            #"電腦專長"
            computer_skill = computer_skill

            #"附加條件"
            ad_condition = ad_condition


            # 儲存
            
            with open(path_csv, mode='a+', newline='', encoding='utf-8') as employee_file: #w
                employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                employee_writer.writerow([get_date, title, company, address, get_mone, jbInfoTxt, location, jobs_exp, education, jobs_url,jobdetail, jobclass, job_skill, computer_skill, ad_condition, industry])

    completed_path_csv = path_csv.with_name(f"{today}{key_txt}_1111人力銀行.csv")
    os.rename(path_csv, completed_path_csv)

    return print('爬取1111完成：請開啟csv檔案')


def crawler_1111():

    crawl_list = ['軟體工程師', 'Software Developer', '通訊軟體工程師', '韌體工程師', 'Firmware Engineer',
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
    
    def check_completed_keywords(crawl_list):
      #先去看近期有沒有抓過，但執行失敗
      folder_path = Path(__file__).parent.parent/ 'tasks'/'raw_data_1111'
      completed_keywords = []
      for keyword in crawl_list:
        # 使用通配符來匹配日期部分，查找 OK 檔案
        file_pattern = folder_path / f"*{keyword}_1111人力銀行.csv"
        csv_files = list(folder_path.glob(file_pattern.name))  # 轉為 list 以便檢查是否有檔案

        # 如果找到符合的檔案，表示該 keyword 已經完成
        if csv_files:
            completed_keywords.append(keyword)

        # 剔除已完成的 keywords
      remaining_keywords = [kw for kw in crawl_list if kw not in completed_keywords]
      
      # 刪除所有以 _TBD.csv 結尾的文件
      tbd_files = folder_path.glob("*1111人力銀行_TBD.csv")
      for tbd_file in tbd_files:
        try:
          tbd_file.unlink()  # 刪除文件
          print(f"Deleted file: {tbd_file}")
        except Exception as e:
          print(f"Could not delete file {tbd_file}: {e}")

      return remaining_keywords

    # 主執行邏輯
    crawl_list_rm = check_completed_keywords(crawl_list)

    for keyword in crawl_list_rm:
      print(f"正在處理: {keyword}")

      # 執行爬蟲
      find_title_1111(keyword)
      
      # 刪除已處理的 keyword
      crawl_list_rm.remove(keyword)
      print(f"剩下 {len(crawl_list_rm)} 個")
