import pandas as pd
import os
import glob
from pathlib import Path
import csv
import jieba
from datetime import date


def add_column():
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

    folder_path = Path(__file__).parent.parent/ 'tasks'/'raw_data_1111' 

       
    #讀取CSV檔案 
    for keyword in keywords_list:
    
        # 使用通配符來匹配日期部分
        file_pattern = folder_path / f"*{keyword}_1111人力銀行.csv"
        
        # 使用 glob 搜尋符合的檔案
        csv_files = folder_path.glob(file_pattern.name)
        csv_files_found = False

        for csv_file in csv_files:
            csv_files_found = True
            try:
                # 讀取 CSV 檔案
                data = pd.read_csv(csv_file)
            except Exception as e:
                print(f"讀取檔案 {csv_file} 時出錯: {e}，跳過...")
                continue

            # 新增欄位
            data["source"] = "1111"
            data["category_primary"] = next((main_cat for main_cat, keywords in category_dict.items() if keyword in keywords), pd.NA)
            data["category_secondary"] = next((sub_cat for sub_cat, sub_keywords in subcategory_dict.items() if keyword in sub_keywords), pd.NA)
            
            # 覆蓋原檔案
            data.to_csv(csv_file, index=False)
            print(f"成功覆蓋 {csv_file}")

        if not csv_files_found:
            print(f"找不到符合的檔案：{keyword}，跳過...")

def merge_all():
    # 設定資料夾路徑
    folder_path = Path(__file__).parent.parent/ 'tasks'/'raw_data_1111'

    # 初始化空的DataFrame
    combined_df = pd.DataFrame()

    # 迴圈遍歷資料夾內的所有檔案
    for file_path in folder_path.glob('*.csv'):
        df = pd.read_csv(file_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # 將合併後另存
    combined_df.to_csv(folder_path / 'merge_all.csv', index=False)
    print("完成merge_all")


def trans_jieba():
    folder_path = Path(__file__).parent.parent/ 'tasks'/'raw_data_1111'
    file_path = Path(__file__).parent.parent / 'tasks'/'raw_data_1111'/'merge_all.csv'
    
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except pd.errors.ParserError:
        print(f"Error: Unable to parse the CSV file at {file_path}. Check the file format.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    """處理資料內有一些奇怪的字元"""

    df["工作內容(詳細)"]=df["工作內容(詳細)"].replace(u"\xa0","")
    df["附加條件"]=df["附加條件"].str.replace("\t","")
    df.rename(columns={"工作內容(詳細)": "工作內容詳細"}, inplace=True)
    df.rename(columns={"工作內容(簡)": "工作內容簡"}, inplace=True)

    def get_job_skills(text):
        dict_path= Path(__file__).parent.parent / 'tasks'/'word_dict'/'dict.txt.big'
        jieba.set_dictionary(dict_path)
        #把文字做斷字
        seg_words_list = jieba.lcut(text)
        #把斷字和我們自己建的關鍵字清單去做比對，存進word_select_list
        #同一個工作不能重複出現相同的工作技能，所以用set()
        word_select_list=set()
        lowercase_path = Path(__file__).parent.parent / 'tasks'/'word_dict' / 'lowercase_detail_word.txt'
        with open(file= lowercase_path , mode='r', encoding='utf-8') as file:
            detail_words = file.read().split('\n')
        for term in seg_words_list:
            #防止有大小寫差別，一律小寫
            term = term.lower()
            if term in detail_words:
                word_select_list.add(term)

        #word_select_list轉回字串存回去，用逗號隔開
        job_sep = ",".join(word_select_list)
        return job_sep

    #先把NaN換掉，方便後續處理
    df.fillna('', inplace=True)

    #拆解工作內容詳細
    df["工作技能分割1"]=df["工作內容詳細"].apply(get_job_skills)
    df["工作技能分割2"]=df["工作內容簡"].apply(get_job_skills)
    df["工作技能分割3"]=df["附加條件"].apply(get_job_skills)

    #工作內容詳細，工作內容簡,附加條件,電腦專長合併成同一欄
    #電腦專長要轉小寫
    df['電腦專長'] = df['電腦專長'].str.lower()
    df['電腦技能合併後'] = df['工作技能分割1'].fillna('') + ',' + df['工作技能分割2'].fillna('')+ ',' + df['工作技能分割3'].fillna('')+ ',' + df['電腦專長'].fillna('')
    #用set來去除重複的
    df['電腦技能合併後'] = df['電腦技能合併後'].apply(lambda x: ','.join(set(x.split(','))))

    #去除過渡的欄位
    df.drop(columns=['工作技能分割1', '工作技能分割2', '工作技能分割3', '電腦專長',"工作內容詳細","工作內容簡","附加條件"], inplace=True)

    #把"電腦技能合併"後分行
    df['電腦技能合併後']=df['電腦技能合併後'].str.split(',')
    #去除開頭空字串
    df['電腦技能合併後'] = df['電腦技能合併後'].apply(lambda x: x[1:] if x[0].strip() == '' else x)

    df=df.explode('電腦技能合併後')

    #先把NaN換掉，方便後續處理
    df.fillna('', inplace=True)

    #職務欄位分行
    df['職務類別'] = df['職務類別'].str.split(',')
    df['職務類別'] = df['職務類別'].apply(lambda x: x[1:] if (x[0].strip() == '' or pd.isna(x[0])) else x)
    df=df.explode('職務類別')

    #工作技能欄位分行
    df['工作技能']=df['工作技能'].str.split(',')
    df['工作技能'] = df['工作技能'].apply(lambda x: x[1:] if x[0].strip() == '' else x)
    df=df.explode('工作技能')

    #把所有的特殊字源替換，方便上傳Bigquery
    df = df.replace(r'\(', '', regex=True).replace(r'\)', '', regex=True)
    df = df.replace(r'\【', '', regex=True).replace(r'\】', '', regex=True)
    df = df.replace(r'\「', '', regex=True).replace(r'\」', '', regex=True)
    df = df.replace(r'\,', '', regex=True)
    df = df.replace(r'\r', '', regex=True)
    df = df.replace(r'\n', '', regex=True)
    df = df.replace(r'\s', '', regex=True)
    df = df.replace(r'\t', '', regex=True)
    df.columns = df.columns.str.replace(r'[()\s]', '', regex=True)

    #處理薪資有逗號
    df['薪資'] = df['薪資'].str.replace(',', '').replace('無',0).astype(float)

    #去除換行符號
    df = df.replace('\n', ' ', regex=True)

    # 創建中文欄位與英文欄位的對應字典
    columns_mapping = {
    '日期': 'report_date',
    '工作名稱': 'job_title',
    '公司名稱': 'company_name',
    '職務類別': 'job_category',
    '薪資': 'salary',
    '地區': 'location_region',
    '公司地址':'company_loc',
    '經歷': 'experience',
    '學歷': 'education',
    '工作網址': 'job_url',
    '公司產業類別': 'industry',
    '工作技能': 'job_skills',
    '電腦技能合併後': 'tools',
    }

    # 使用字典來重命名欄位
    df = df.rename(columns=columns_mapping)
    #去除不需要的欄位(11/1 討論後)
    df.drop(columns=["company_loc","education"])

    df.to_csv(folder_path /'all_raw_data_1111.csv',index=False)

def trans_data_1111():
    #增加分類欄位
    add_column()
    #合併欄位
    merge_all()
    #jieba 處理字串
    trans_jieba()
