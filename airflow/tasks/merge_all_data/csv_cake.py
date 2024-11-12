import pandas as pd
import re
from pathlib import Path
def csv_cake():
  # 讀取CSV文件
  df = pd.read_csv('opt/airflow/tasks/raw_data4merge/all_raw_data_cake.csv',encoding= 'utf-8-sig')

  df = df.drop(columns=["url","category_primary"])
  df = df.rename(columns={'category_secondary':'job_category'})
  df.insert(0, 'source','Cake')
  df.insert(4,'education',None)
  # 指定新的欄位順序
  new_order = ['source', 'report_date', 'job_title','company_name','education','job_category','salary','location_region','experience','industry','tools']
    # 替換為你想要的欄位順序
  df = df[new_order]


  # 定義正則表達式來匹配城市或縣名
  pattern = r'(台北市|臺北市|新北市|基隆市|新竹市|桃園市|新竹縣|宜蘭縣|臺中市\
    |苗栗縣|彰化縣|南投縣|雲林縣|高雄市|臺南市|嘉義市|嘉義縣|屏東縣|澎湖縣|Taipei City\
      |台中市|苗栗市|台南市|中南美洲|上海市|江蘇省|澳門|香港|美加地區|美國\
      |中正區|信義區|內湖區|大同區|大安區|中山區|民生東路|松山區|台東縣|台南市|臺南市\
        |日本|東北亞|南韓|東南亞|泰國|馬來西亞|菲律賓|越南|新加坡|花蓮縣|金門縣|東非|南投縣|印度\
          |苗栗縣|桃園市|澳洲|高雄市|新店區|汐止區|板橋區|新竹縣|嘉義市|彰化縣|中歐\
            |西歐|荷蘭|德國|亞洲|北美洲|)'




  # 函數來提取地址中的城市或縣
  def extract_and_replace_city(address):
      # 檢查 address 是否為字串型別
      if isinstance(address, str):
          # 在地址中查找匹配的城市名稱
          match = re.search(pattern, address)
          if match:
              # 如果有匹配，僅返回匹配到的城市名稱
              return match.group(0)
      return address  # 若不是字串或沒有找到匹配，則返回原值

  # 對地址欄位應用函數，將結果更新回原欄位
  df['location_region'] = df['location_region'].apply(extract_and_replace_city)

  df['location_region'] = df['location_region'].str.replace('臺北市', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('Taipei City', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('上海市', '中港澳', regex=False)
  df['location_region'] = df['location_region'].str.replace('江蘇省', '中港澳', regex=False)
  \
  df['location_region'] = df['location_region'].str.replace('澳門', '中港澳', regex=False)
  df['location_region'] = df['location_region'].str.replace('香港', '中港澳', regex=False)
  df['location_region'] = df['location_region'].str.replace('美加地區', '北美洲', regex=False)
  df['location_region'] = df['location_region'].str.replace('美國', '北美洲', regex=False)

  df['location_region'] = df['location_region'].str.replace('臺中市', '台中市', regex=False)
  df['location_region'] = df['location_region'].str.replace('中正區', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('信義區', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('內湖區', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('大同區', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('大安區', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('中山區', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('民生東路', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('松山區', '台北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('臺南市', '台南市', regex=False)
  df['location_region'] = df['location_region'].str.replace('日本', '東北亞', regex=False)
  df['location_region'] = df['location_region'].str.replace('南韓', '東北亞', regex=False)
  df['location_region'] = df['location_region'].str.replace('泰國', '東南亞', regex=False)
  df['location_region'] = df['location_region'].str.replace('馬來西亞', '東南亞', regex=False)
  df['location_region'] = df['location_region'].str.replace('菲律賓', '東南亞', regex=False)
  df['location_region'] = df['location_region'].str.replace('越南', '東南亞', regex=False)
  df['location_region'] = df['location_region'].str.replace('新加坡', '東南亞', regex=False)
  df['location_region'] = df['location_region'].str.replace('東非', '非洲', regex=False)
  df['location_region'] = df['location_region'].str.replace('印度', '南亞', regex=False)

  df['location_region'] = df['location_region'].str.replace('澳洲', '紐澳', regex=False)
  df['location_region'] = df['location_region'].str.replace('新店區', '新北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('汐止區', '新北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('板橋區', '新北市', regex=False)
  df['location_region'] = df['location_region'].str.replace('中歐', '歐洲', regex=False)
  df['location_region'] = df['location_region'].str.replace('西歐', '歐洲', regex=False)
  df['location_region'] = df['location_region'].str.replace('荷蘭', '歐洲', regex=False)
  df['location_region'] = df['location_region'].str.replace('德國', '歐洲', regex=False)



  def transform_experience(text):
      if isinstance(text, str):  # 確認 text 是否為字串
          if re.search(r"需具備\s*(\d+)\s*年以上工作經驗", text):  
            text = re.sub(r'需具備\s*(\d+)\s*年以上工作經驗', r'\1', text)
          elif re.search(r"(\d+)\s+years\s+of\s+experience\s+required", text): 
            text = re.sub(r'(\d+)\s+years\s+of\s+experience\s+required', r'\1', text)
          # 將「No requirement for relevant working experience」替換為「無經驗可」
          elif re.search(r"不限年資", text):
              text = 0
          elif re.search(r"No requirement for relevant working experience", text):
              text = 0
          else:
              text = 0
          
        
      return text

  # 對整個欄位進行替換
  df['experience'] = df['experience'].apply(transform_experience)
  df['experience'] = df['experience'].fillna('0')

  # def transform_salary(text):
  #     if isinstance(text, str):
  #         # 處理年薪範圍
  #         if "TWD / year" in text:
  #             # 移除 TWD 和 / year
  #             text = re.sub(r"\s*TWD\s*/\s*year", "", text)
  #             # 提取薪資範圍內的最高值
  #             match = re.search(r"~\s*([\d,]+)", text)
  #             if match:
  #                 # 取得最高值並去掉逗號
  #                 max_salary = int(match.group(1).replace(",", ""))
  #                 # 計算每月薪資
  #                 return max_salary // 12
  #         # 處理月薪情況
  #         elif "TWD / month" in text:
  #             # 處理範圍月薪，提取數字範圍的最高值
  #             match = re.search(r"~\s*([\d,]+)", text)
  #             if match:
  #                 # 取得最高值並去掉逗號
  #                 max_salary = int(match.group(1).replace(",", ""))
  #                 return max_salary
  #     return None  # 如果不符合格式則返回 None

  # # 對欄位進行轉換
  # df['salary'] = df['salary'].apply(transform_salary)

  # 將調整後的DataFrame寫回CSV
  pattern2 = r'(大學|專科|碩士|高中|不拘)'


  # 函數來提取地址中的城市或縣
  def extract_and_replace_education(education):
      # 檢查 address 是否為字串型別
      if isinstance(education, str):
          # 在地址中查找匹配的城市名稱
          match = re.search(pattern2, education)
          if match:
              # 如果有匹配，僅返回匹配到的城市名稱
              return match.group(0)
      return education  # 若不是字串或沒有找到匹配，則返回原值

  # 對地址欄位應用函數，將結果更新回原欄位
  df['education'] = df['education'].apply(extract_and_replace_education)
  df['education'] = df['education'].str.replace('學歷不拘', '不拘', regex=False)



  pattern3= r'(農|林|漁|牧|石油|礦|天然氣|IC|製造|加工|用水\
      |衛生|工程|營造|批發|零售|百貨|客運|貨運|物流|休閒|飯店\
          |餐館|電信|通信|整合|網路|廣播|數位|保險|金融\
          |不動產|服務|研發|設計|人力|政府|教育|補習\
              |醫療|醫院|藝術|遊戲|運動|協會|美容|寵物|軟體)'


  # 函數來提取地址中的城市或縣
  def extract_and_replace_industry(industry):
      # 檢查 address 是否為字串型別
      if isinstance(industry, str):
          # 在地址中查找匹配的城市名稱
          match = re.search(pattern3, industry)
          if match:
              # 如果有匹配，僅返回匹配到的城市名稱
              return match.group(0)
          else:
              return None
      return industry  # 若不是字串或沒有找到匹配，則返回原值
  df['industry'] = df['industry'].astype(str)
  # 對地址欄位應用函數，將結果更新回原欄位
  df['industry'] = df['industry'].apply(extract_and_replace_industry)
  df['industry'] = df['industry'].str.replace('農', '農林漁牧業', regex=False)
  df['industry'] = df['industry'].str.replace('林', '農林漁牧業', regex=False)
  df['industry'] = df['industry'].str.replace('漁', '農林漁牧業', regex=False)
  df['industry'] = df['industry'].str.replace('牧', '農林漁牧業', regex=False)
  df['industry'] = df['industry'].str.replace('石油', '礦業及土石採取業', regex=False)
  df['industry'] = df['industry'].str.replace('礦', '礦業及土石採取業', regex=False)
  df['industry'] = df['industry'].str.replace('天然氣', '礦業及土石採取業', regex=False)
  df['industry'] = df['industry'].str.replace('IC', '製造業', regex=False)
  df['industry'] = df['industry'].str.replace('製造', '製造業', regex=False)
  df['industry'] = df['industry'].str.replace('加工', '製造業', regex=False)
  df['industry'] = df['industry'].str.replace('用水', '用水供應及污染整治業', regex=False)
  df['industry'] = df['industry'].str.replace('衛生', '用水供應及污染整治業', regex=False)
  df['industry'] = df['industry'].str.replace('工程', '營建工程業', regex=False)
  df['industry'] = df['industry'].str.replace('營造', '營建工程業', regex=False)
  df['industry'] = df['industry'].str.replace('批發', '批發及零售業', regex=False)
  df['industry'] = df['industry'].str.replace('零售', '批發及零售業', regex=False)
  df['industry'] = df['industry'].str.replace('百貨', '批發及零售業', regex=False)
  df['industry'] = df['industry'].str.replace('客運', '運輸及倉儲業', regex=False)
  df['industry'] = df['industry'].str.replace('貨運', '運輸及倉儲業', regex=False)
  df['industry'] = df['industry'].str.replace('物流', '運輸及倉儲業', regex=False)
  df['industry'] = df['industry'].str.replace('休閒', '住宿及餐飲業', regex=False)
  df['industry'] = df['industry'].str.replace('飯店', '住宿及餐飲業', regex=False)
  df['industry'] = df['industry'].str.replace('餐館', '住宿及餐飲業', regex=False)
  df['industry'] = df['industry'].str.replace('電信', '出版影音及資通訊業', regex=False)
  df['industry'] = df['industry'].str.replace('通信', '出版影音及資通訊業', regex=False)
  df['industry'] = df['industry'].str.replace('整合', '出版影音及資通訊業', regex=False)
  df['industry'] = df['industry'].str.replace('網路', '出版影音及資通訊業', regex=False)
  df['industry'] = df['industry'].str.replace('廣播', '出版影音及資通訊業', regex=False)
  df['industry'] = df['industry'].str.replace('數位', '出版影音及資通訊業', regex=False)
  df['industry'] = df['industry'].str.replace('保險', '金融及保險業', regex=False)
  df['industry'] = df['industry'].str.replace('金融', '金融及保險業', regex=False)
  df['industry'] = df['industry'].str.replace('財務', '金融及保險業', regex=False)
  df['industry'] = df['industry'].str.replace('不動產', '不動產業', regex=False)
  df['industry'] = df['industry'].str.replace('服務', '專業、科學及技術服務業', regex=False)
  df['industry'] = df['industry'].str.replace('研發', '專業、科學及技術服務業', regex=False)
  df['industry'] = df['industry'].str.replace('設計', '專業、科學及技術服務業', regex=False)
  df['industry'] = df['industry'].str.replace('人力', '支援服務業', regex=False)
  df['industry'] = df['industry'].str.replace('政府', '公共行政及國防', regex=False)
  df['industry'] = df['industry'].str.replace('教育', '教育業', regex=False)
  df['industry'] = df['industry'].str.replace('補習', '教育業', regex=False)
  df['industry'] = df['industry'].str.replace('醫療', '醫療保健及社會工作服務業', regex=False)
  df['industry'] = df['industry'].str.replace('醫院', '醫療保健及社會工作服務業', regex=False)
  df['industry'] = df['industry'].str.replace('藝術', '藝術、娛樂及休閒服務業', regex=False)
  df['industry'] = df['industry'].str.replace('遊戲', '藝術、娛樂及休閒服務業', regex=False)
  df['industry'] = df['industry'].str.replace('運動', '藝術、娛樂及休閒服務業', regex=False)
  df['industry'] = df['industry'].str.replace('協會', '其他服務業', regex=False)
  df['industry'] = df['industry'].str.replace('美容', '其他服務業', regex=False)
  df['industry'] = df['industry'].str.replace('寵物', '其他服務業', regex=False)
  df['industry'] = df['industry'].str.replace('軟體', '專業、科學及技術服務業', regex=False)

  df['salary'] = df['salary'].astype(float)
  df.to_csv('opt/airflow/tasks/trans_data/data_cake.csv', index=False, encoding='utf-8-sig')
