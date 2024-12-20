import pandas as pd
import re
from pathlib import Path
def csv_1111():
    # 讀取CSV文件
    df = pd.read_csv('opt/airflow/tasks/raw_data4merge/all_raw_data_1111.csv',encoding= 'utf8')

    df = df.drop(columns=['job_url','job_skills','company_loc','category_primary','category_secondary'])
    # 指定新的欄位順序

    new_order = ['source', 'report_date', 'job_title','company_name','education','job_category','salary','location_region','experience','industry','tools']
    # 替換為你想要的欄位順序
    df = df[new_order]

    # 將調整後的DataFrame寫回CSV
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
        # 在地址中查找匹配的城市名稱
        match = re.search(pattern, address)
        if match:
            # 如果有匹配，僅返回匹配到的城市名稱
            return match.group(0)
        return address  # 若沒有找到，則返回原地址

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


    # 更新 transform_experience 函數以處理其他無法匹配的情況
    def transform_experience(text):
        if isinstance(text, str):
            # 如果有 "X年工作經驗" 或 "X年工作經驗以上"，進行替換
            if re.search(r"(\d+)\s*年工作經驗", text):
                # 先處理 "X年工作經驗以上" 替換為 "X年以上"
                text = re.sub(r"(\d+)\s*年工作經驗以上", r"\1", text)


            elif re.search(r"經驗不拘", text):
                text = 0
            else:
                text = 0  # 如果都不符合，將其替換為空值
        return text

    # 對整個欄位進行替換
    df['experience'] = df['experience'].apply(transform_experience)
    df['experience'] = df['experience'].fillna('0')

    # 定義正則表達式模式

    pattern_inner_comma = re.compile(r'/')

    # 使用正則表達式修改 'tools' 欄位
    def clean_tools(tools_value):
        if pd.notna(tools_value):  # 檢查非空值
            # 刪除最前面的逗號並替換內部的逗號為頓號
            
            tools_value = pattern_inner_comma.sub('、', tools_value)  # 將內部逗號替換為頓號
            
            # 移除空格並將 'MS SQL' 轉換為 'MSSQL'
            tools_value = re.sub(r'\s+', '', tools_value)  # 移除所有空白字符
        return tools_value

    # 應用清理函數到 'tools' 欄位
    df['tools'] = df['tools'].apply(clean_tools)
    # 假設 'report_date' 欄位包含日期，將其轉換為 'YYYY-MM-DD' 格式
    df['report_date'] = pd.to_datetime(df['report_date'], format='%Y/%m/%d', errors='coerce')
    df['salary'] = df['salary'].astype(float)

    pattern2 = r'(高中|專科大學|碩士|不拘)'


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





    pattern3= r'(農|林|漁|牧|石油|礦|天然氣|IC|製造業|加工|用水\
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


    df.to_csv('opt/airflow/tasks/trans_data/data_1111.csv', index=False, encoding='utf8')