import pandas as pd
import os
import re
import numpy as np
from tqdm import tqdm
import logging
from pathlib import Path

# 設定 logging 配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# 定義分隔符號
delimiters = {
    '句號': ['。', '.'],
    '逗號': ['，', ','],
    '分號': ['；', ';'],
    '冒號': ['：', ':'],
    '問號': ['？', '?'],
    '感嘆號': ['！', '!'],
    '連接號': ['－', '-'],
    '空格': ['　', ' '],
    '換行符': ['\n'],
    '制表符': ['\t'],
    '斜線': ['／', '/'],
    '反斜線': ['＼', '\\'],
    '直線': ['｜', '|'],
    '底線': ['＿', '_'],
    '星號': ['＊', '*'],
    '百分號': ['％', '%'],
    '小括號': ['（', '）', '(', ')'],
    '中括號': ['［', '］', '[', ']'],
    '大括號': ['｛', '｝', '{', '}'],
    '單引號': ['’', "'", '‘'],
    '雙引號': ['“', '"', '”'],
    '重音符號': ['｀', '`'],
    '波浪線': ['～', '~'],
    '省略號': ['……', '...'],
    '井號': ['＃', '#'],
    '和號': ['＆', '&'],
    '加號': ['＋', '+'],
    '數字圖形': ['➊', '➋', '➌', '➍', '➎', '➏', '➐', '➑', '➒', '①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨'],
    '其他': ['●', '．', '▍', '◆'],
    '尖括號': ['＜', '＞', '<', '>'],
    '頓號': ['、']
}

def process_report_date(date):
    if pd.isnull(date):
        return np.nan  # 或者可以設為其他預設值，例如 '不拘'
    try:
        # 解析日期，移除已棄用的 infer_datetime_format 參數
        parsed_date = pd.to_datetime(date, errors='coerce')
        if pd.isnull(parsed_date):
            return np.nan  # 或者可以設為其他預設值，例如 '不拘'
        else:
            # 轉換為 yyyy/mm/dd 格式
            formatted_date = parsed_date.strftime('%Y/%m/%d')
            return formatted_date
    except:
        return np.nan  # 或者可以設為其他預設值，例如 '不拘'

def remove_spaces_hyphens_periods(x):
    if isinstance(x, str):
        return x.replace(' ', '').replace('　', '').replace('-', '').replace('－', '').replace('.', '').replace('。', '')
    elif isinstance(x, list):
        return [item.replace(' ', '').replace('　', '').replace('-', '').replace('－', '').replace('.', '').replace('。', '') for item in x]
    else:
        return x

def convert_english_to_lowercase(text_list):
    if isinstance(text_list, list):
        return [item.lower() if re.search(r'[A-Za-z]', item) else item for item in text_list]
    elif isinstance(text_list, str):
        return text_list.lower() if re.search(r'[A-Za-z]', text_list) else text_list
    else:
        return text_list

def filter_keywords(text_list, keywords_set, stopwords_set):
    if isinstance(text_list, list):
        # 過濾：保留在關鍵字清單中且不在停用詞清單中的項目
        filtered = [item for item in text_list if item in keywords_set and item not in stopwords_set]
        return filtered if filtered else np.nan  # 如果結果為空，設為 NaN
    elif isinstance(text_list, str):
        # 假設如果是字符串，進行分割後過濾
        parts = split_by_delimiters(text_list)
        filtered = [item for item in parts if item in keywords_set and item not in stopwords_set]
        return filtered if filtered else np.nan
    else:
        return np.nan  # 非列表或字符串類型，設為 NaN

def process_experience(text):
    if not isinstance(text, str):
        return '不拘'
    text = text.strip()

    # 1. 檢查是否包含 "一般求職者"
    if "一般求職者" in text:
        return '不拘'

    # 2. 檢查是否包含 "無需經驗"
    if '無需經驗' in text:
        return '不拘'

    # 3. 搜尋是否有 'N年以上'，允許中間有其他字符
    experience_match = re.search(r'(\d+)\s*年以上', text)
    if experience_match:
        return f"{experience_match.group(1)}年以上"

    # 4. 搜尋是否有 'N年'，允許中間有其他字符
    year_match = re.search(r'(\d+)\s*年', text)
    if year_match:
        return f"{year_match.group(1)}年以上"

    # 5. 如果沒有匹配，嘗試從文本中提取數字並加上 "年以上"
    number_match = re.search(r'(\d+)', text)
    if number_match:
        return f"{number_match.group(1)}年以上"

    # 6. 如果仍然沒有匹配，返回 '不拘'
    return '不拘'

def remove_number_years(majors):
    if isinstance(majors, list):
        # 移除符合 '數字 + 年以上' 的元素
        return [m for m in majors if not re.match(r'\d+年以上', m)]
    return majors

def process_required_major(majors):
    if isinstance(majors, list):
        # 1. 移除符合 '數字 + 年以上' 的元素
        majors = remove_number_years(majors)

        # 2. 檢查是否任何元素是數字
        if any(element.isdigit() for element in majors):
            return ['不拘']

        # 3. 檢查是否所有元素都是 '以上'
        if all(element == '以上' for element in majors):
            return ['不拘']

    return majors

def contains_chinese(text):
    return bool(re.search(r'[\u4e00-\u9fff]', text))

def split_english_chinese(text):
    # 拆分英文和中文部分，並保留 "+" 或 "++"
    english_parts = re.findall(r'[A-Za-z][A-Za-z0-9]*\+*\+*', text)
    chinese_parts = re.findall(r'[\u4e00-\u9fff]+', text)
    return english_parts, chinese_parts

def move_skills_and_requirements(row):
    # 初始化集合以避免重複
    new_job_skills = set()
    new_other_requirements = set()

    # 處理 job_skills
    if isinstance(row['job_skills'], list):
        for skill in row['job_skills']:
            if contains_chinese(skill):
                # 如果包含中文，拆分並移動
                eng, chi = split_english_chinese(skill)
                if eng:
                    new_job_skills.update(eng)
                if chi:
                    new_other_requirements.update(chi)
            else:
                # 只有英文，保留在 job_skills
                new_job_skills.add(skill)
    elif isinstance(row['job_skills'], str) and pd.notnull(row['job_skills']):
        # 單一字串處理
        skill = row['job_skills']
        if contains_chinese(skill):
            eng, chi = split_english_chinese(skill)
            if eng:
                new_job_skills.update(eng)
            if chi:
                new_other_requirements.update(chi)
        else:
            new_job_skills.add(skill)

    # 處理 other_requirements
    if isinstance(row['other_requirements'], list):
        for req in row['other_requirements']:
            if contains_chinese(req):
                # 只有中文，保留在 other_requirements
                new_other_requirements.add(req)
            elif not contains_chinese(req):
                # 只有英文，移動到 job_skills
                new_job_skills.add(req)
            else:
                # 同時包含中文和英文，拆分
                eng, chi = split_english_chinese(req)
                if eng:
                    new_job_skills.update(eng)
                if chi:
                    new_other_requirements.update(chi)
    elif isinstance(row['other_requirements'], str) and pd.notnull(row['other_requirements']):
        # 單一字串處理
        req = row['other_requirements']
        if contains_chinese(req):
            new_other_requirements.add(req)
        elif not contains_chinese(req):
            new_job_skills.add(req)
        else:
            eng, chi = split_english_chinese(req)
            if eng:
                new_job_skills.update(eng)
            if chi:
                new_other_requirements.update(chi)

    # 特別處理英文字母後有 "+" 或 "++" 的情況
    def process_skill(skill):
        # 保留字母後的 "+" 或 "++"
        match = re.match(r'^([A-Za-z][A-Za-z0-9]*)(\+*)$', skill)
        if match:
            main_skill = match.group(1)
            pluses = match.group(2)
            return main_skill + pluses
        return skill

    new_job_skills = {process_skill(s) for s in new_job_skills}

    # 刪除單獨的小寫英文字母 a-z
    new_job_skills = {s for s in new_job_skills if not re.fullmatch(r'[a-z]', s)}

    # 更新行的 job_skills 和 other_requirements，轉換為列表並排序
    row['job_skills'] = sorted(new_job_skills) if new_job_skills else np.nan

    # 特別處理 other_requirements 中的 '不拘'
    if '不拘' in new_other_requirements:
        if len(new_other_requirements) > 1:
            new_other_requirements.discard('不拘')  # 有其他內容，刪除 '不拘'
        # 如果只有 '不拘'，則保留

    row['other_requirements'] = sorted(new_other_requirements) if new_other_requirements else '不拘'

    return row

def remove_delimiters(text):
    if not isinstance(text, str):
        return text
    # 將所有分隔符號合併成一個正則表達式模式
    # 使用 re.escape 來轉義特殊字符
    all_delimiters = [re.escape(d) for sublist in delimiters.values() for d in sublist]
    pattern = '[' + ''.join(all_delimiters) + ']'
    # 使用 re.sub 將所有分隔符號替換為空字符串
    return re.sub(pattern, '', text)

def replace_nulls(x):
    if pd.isnull(x):
        return '不拘'
    elif isinstance(x, list) and len(x) == 0:
        return '不拘'
    elif isinstance(x, str) and x.strip() == '':
        return '不拘'
    else:
        return x

def replace_education_terms(x):
    if isinstance(x, list):
        # 對列表中的每個元素進行替換
        return ['不拘' if re.search(r'\d+年以上', item) or item == '無限制' else item for item in x]
    elif isinstance(x, str):
        # 替換單一字串
        return '不拘' if re.search(r'\d+年以上', x) or x == '無限制' else x
    else:
        return x

def process_location(row):
    if pd.notnull(row['company_loc']):
        # 1. 移除所有的 '加工區' 字元
        company_loc_clean = row['company_loc'].replace('加工區', '')

        # 2. 移除所有定義的分隔符號
        company_loc_clean = remove_delimiters(company_loc_clean)

        # 3. 使用正則表達式匹配所有 "XX市XX區", "XX縣XX區", "XX縣XX鎮" 的部分
        pattern_full = re.compile(r'[\u4e00-\u9fff]{1,3}(?:市|縣)[\u4e00-\u9fff]{1,3}(?:區|鎮)')
        pattern_zone = re.compile(r'[\u4e00-\u9fff]+(?:區|鎮)')
        matches = pattern_full.findall(company_loc_clean)

        if matches:
            # 4. 移除重複匹配項（保留順序且移除重複）
            seen_zones = {}
            unique_matches = []
            for match in matches:
                zone_match = pattern_zone.search(match)
                if zone_match:
                    zone = zone_match.group(0)
                    if zone not in seen_zones:
                        unique_matches.append(match)
                        seen_zones[zone] = True

            # 5. 組合所有匹配項，使用逗號分隔
            location_region = ','.join(unique_matches)

            # 6. 更新 location_region 欄位
            row['location_region'] = location_region

            # 7. 從 company_loc 中移除所有匹配項
            for match in matches:
                row['company_loc'] = row['company_loc'].replace(match, '').strip()

        # 如果沒有匹配項，根據需求設置 location_region
        else:
            row['location_region'] = np.nan  # 或者 '不拘' 等其他預設值

    return row

def process_text_field(text):
    if not isinstance(text, str):
        return text

    # 定義要刪除的括號類型
    bracket_patterns = [
        r'\(.*?\)',  # ()
        r'【.*?】',   # 【】
        r'\[.*?\]',   # []
        r'\{.*?\}',   # {}
        r'<.*?>'      # <>
    ]

    # 依序刪除所有括號內的內容
    for pattern in bracket_patterns:
        text = re.sub(pattern, '', text)

    # 刪除剩餘的單獨特殊符號（如果有）
    text = re.sub(r'[【】\[\]\(\)\{\}<>]', '', text)

    # 刪除 '/' 和 '\\' 及其後面的內容
    text = re.split(r'[\\/]', text)[0].strip()

    # 移除多餘的空白字符
    text = text.strip()

    # 如果刪除後 text 為空，設為 '不拘'
    if not text:
        return '不拘'

    return text

def split_by_delimiters(text):
    if not isinstance(text, str):
        return text
    # 將所有分隔符號合併成一個正則表達式模式
    all_delimiters = [re.escape(d) for sublist in delimiters.values() for d in sublist]
    pattern = '|'.join(all_delimiters)
    # 使用 re.split 進行分割
    parts = re.split(pattern, text)
    # 移除空字符串並去除前後空白
    parts = [part.strip() for part in parts if part.strip()]
    return parts if parts else np.nan

def process_multiple_text_fields(row, fields):
    for field in fields:
        if field in row and pd.notnull(row[field]):
            processed = process_text_field(row[field])
            row[field] = processed
    return row

def process_education_and_company_loc(row):
    fields_to_process = ['education', 'company_loc', 'job_title']
    row = process_multiple_text_fields(row, fields_to_process)
    row = process_location(row)  # 確保處理 location_region
    return row

def move_job_description_to_job_skills(row):
    # 確保 'job_description' 和 'job_skills' 欄位存在
    if 'job_description' in row and 'job_skills' in row:
        job_description = row['job_description']
        job_skills = row['job_skills']

        # 將 'job_description' 轉換為列表
        if isinstance(job_description, list):
            job_description_list = job_description
        elif isinstance(job_description, str):
            job_description_list = [job_description]
        else:
            job_description_list = []

        # 將 'job_skills' 轉換為列表
        if isinstance(job_skills, list):
            job_skills_list = job_skills
        elif isinstance(job_skills, str):
            job_skills_list = [job_skills]
        else:
            job_skills_list = []

        # 合併兩個列表
        combined_skills = job_skills_list + job_description_list

        # 移除重複項目，保留順序
        seen = set()
        deduplicated_skills = []
        for skill in combined_skills:
            if skill not in seen:
                seen.add(skill)
                deduplicated_skills.append(skill)

        # 更新 'job_skills' 欄位
        row['job_skills'] = deduplicated_skills if deduplicated_skills else np.nan

        # 刪除 'job_description' 欄位
        row['job_description'] = np.nan

    return row

def rename_job_skills_to_tools(df):
    if 'job_skills' in df.columns:
        df.rename(columns={'job_skills': 'tools'}, inplace=True)
    return df

def remove_duplicate_tools(row):
    if 'tools' in row and isinstance(row['tools'], list):
        seen = set()
        deduped_tools = []
        for tool in row['tools']:
            if tool not in seen:
                seen.add(tool)
                deduped_tools.append(tool)
        row['tools'] = deduped_tools if deduped_tools else np.nan
    return row

def main():
    # 定義輸入、Jieba 和輸出資料夾路徑
    # 請根據您的 Airflow 執行環境修改此路徑
    INPUT_FOLDER = Path(__file__).parent.parent / 'raw_data_123'/'after_1'   # 修改為適當的路徑
    JIEBA_FOLDER = Path(__file__).parent.parent / 'word_dict'      # 修改為適當的路徑
    OUTPUT_FOLDER = Path(__file__).parent.parent / 'raw_data_123'/'after_2'       # 修改為適當的路徑

    # 確保輸出資料夾存在，若不存在則創建
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # 定義關鍵字和停用詞檔案路徑
    keywords_file = Path(__file__).parent.parent / 'word_dict'/ 'lowercase_detail_word.txt'
    stopwords_file = Path(__file__).parent.parent /'word_dict'/'stop_words.txt'

    # 讀取關鍵字清單並將英文單詞轉為小寫
    try:
        with open(keywords_file, 'r', encoding='utf-8') as f:
            keywords = [
                line.strip().lower() if re.search(r'[A-Za-z]', line.strip()) else line.strip()
                for line in f if line.strip()
            ]
    except Exception as e:
        raise Exception(f"讀取關鍵字檔案時發生錯誤：{e}")

    # 讀取停用詞清單並將英文單詞轉為小寫
    try:
        with open(stopwords_file, 'r', encoding='utf-8') as f:
            stopwords = [
                line.strip().lower() if re.search(r'[A-Za-z]', line.strip()) else line.strip()
                for line in f if line.strip()
            ]
    except Exception as e:
        raise Exception(f"讀取停用詞檔案時發生錯誤：{e}")

    # 將關鍵字和停用詞轉換為集合以提高查詢效率
    keywords_set = set(keywords)
    stopwords_set = set(stopwords)

    # 定義欄位名稱對應的字典（新增 '產業別': 'industry'）
    column_mapping = {
        '日期': 'report_date',
        '職位名稱': 'job_title',
        '公司名稱': 'company_name',
        '工作內容': 'job_description',
        '工作地點': 'company_loc',
        '學歷要求': 'education',
        '科系要求': 'required_major',
        '工作經驗': 'experience',
        '電腦技能': 'job_skills',
        '其他條件': 'other_requirements',
        '職務類別': 'job_category',
        '薪資待遇': 'salary',
        '來源': 'source',
        '大分類': 'category_primary',
        '小分類': 'category_secondary',
        '連結': 'job_url',
        '地區': 'location_region',
        '產業別': 'industry'
    }

    # 遍歷輸入資料夾中的所有 CSV 檔案，使用 tqdm 進度條
    csv_files = [f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith('.csv')]

    for filename in tqdm(csv_files, desc="處理 CSV 檔案"):
        input_path = os.path.join(INPUT_FOLDER, filename)

        try:
            # 讀取CSV
            data = pd.read_csv(input_path, encoding='utf-8')

            # 欄位名稱對應變更
            data.rename(columns=column_mapping, inplace=True)

            # 2. 處理 report_date 欄位，確保格式為 yyyy/mm/dd
            if 'report_date' in data.columns:
                data['report_date'] = data['report_date'].apply(process_report_date)

            # 3. 處理 experience 欄位，確保格式為 'N年以上' 或 '不拘'
            if 'experience' in data.columns:
                data['experience'] = data['experience'].apply(process_experience)

            # 4. 處理 job_title、company_loc 和 education 欄位，刪除括號內的內容並處理特殊符號
            if any(col in data.columns for col in ['job_title', 'company_loc', 'education']):
                data = data.apply(process_education_and_company_loc, axis=1)

            # 5. 移除所有欄位中的半形與全形空格、連接號及句號（不包含 report_date 和 experience）
            columns_to_clean = [col for col in data.columns if col not in ['report_date', 'experience']]

            # 使用 DataFrame.apply 和 Series.map 替代 applymap
            data[columns_to_clean] = data[columns_to_clean].apply(
                lambda col: col.map(remove_spaces_hyphens_periods)
            )

            # 7. 刪除包含英文加四位數字的行
            if 'job_title' in data.columns:
                initial_length = len(data)
                data = data[~data['job_title'].str.match(r'^[A-Za-z]+\d{4}$', na=False)]
                removed_rows = initial_length - len(data)
                if removed_rows > 0:
                    tqdm.write(f"已刪除 {removed_rows} 行包含英文加四位數字的 'job_title'。")

            # 8. 將空值的 required_major、other_requirements 設為 '不拘'
            for col in ['required_major', 'other_requirements']:
                if col in data.columns:
                    data[col] = data[col].apply(replace_nulls)

            # 9. 定義要處理的欄位，包含 'job_description', 'job_skills', 'other_requirements', 'required_major'
            target_columns = ['job_description', 'job_skills', 'other_requirements', 'required_major']

            # 10. 應用英文轉小寫和過濾關鍵字及停用詞
            for col in target_columns:
                if col in data.columns:
                    # 1. 將英文單詞轉為小寫
                    data[col] = data[col].apply(convert_english_to_lowercase)

                    # 2. 應用過濾關鍵字和停用詞
                    data[col] = data[col].apply(lambda x: filter_keywords(x, keywords_set, stopwords_set))

            # 11. 移動 job_skills 和 other_requirements 中的內容
            data = data.apply(move_skills_and_requirements, axis=1)

            # 13. 根據定義的分隔符號分割指定欄位（不包含 job_title）
            split_columns = ['job_description', 'required_major', 'job_skills', 'other_requirements']
            for col in split_columns:
                if col in data.columns:
                    data[col] = data[col].apply(split_by_delimiters)

            # 新增步驟：在 explode 之前，處理 education 欄位
            if 'education' in data.columns:
                data['education'] = data['education'].apply(replace_education_terms)

            # 14. 將 job_description 的內容移動至 job_skills 並刪除 job_description
            data = data.apply(move_job_description_to_job_skills, axis=1)

            # 15. 移除 job_skills 中的重複內容
            data = data.apply(remove_duplicate_tools, axis=1)

            # 17. 將 job_skills 欄位重新命名為 tools
            data = rename_job_skills_to_tools(data)

            # 新增步驟：將 'other_requirements' 欄位重新命名為 'job_skills'
            if 'other_requirements' in data.columns:
                data.rename(columns={'other_requirements': 'job_skills'}, inplace=True)

            # 新增步驟：刪除 'job_description' 和 'company_loc' 欄位
            columns_to_drop = ['job_description', 'company_loc']
            existing_columns_to_drop = [col for col in columns_to_drop if col in data.columns]
            if existing_columns_to_drop:
                data.drop(columns=existing_columns_to_drop, inplace=True)

            # 18. 定義輸出檔案路徑
            output_filename = os.path.splitext(filename)[0] + '_Cleaned.csv'
            cleaned_output_path = os.path.join(OUTPUT_FOLDER, output_filename)

            # 將清洗後的結果儲存為新的CSV
            data.to_csv(cleaned_output_path, index=False, encoding='utf-8-sig')

            logging.info(f"已儲存清洗後的結果到 {cleaned_output_path}")

        except Exception as e:
            tqdm.write(f"處理檔案 '{filename}' 時發生錯誤：{e}")

if __name__ == "__main__":
    main()