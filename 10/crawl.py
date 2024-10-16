import requests
from bs4 import BeautifulSoup
import time
import pandas as pd


# 首頁的 url
url = "https://www.cake.me/jobs"
key = input("關鍵字: ")
all_company_links = []

def fetch_links(page_range):
    for i in range(page_range[0], page_range[1]):
        page_url = f"{url}/{key}?page={i}"
        soup = BeautifulSoup(requests.get(page_url).text, "html.parser")
        links = soup.find_all('a', class_='JobSearchItem_jobTitle__bu6yO')
        for link in links:
            href = link.get('href')
            all_company_links.append(href)
        time.sleep(5)  # 暫停5秒

for start in range(1, 101, 20):
    fetch_links((start, start + 20))

links_list = [f"https://www.cake.me/{a}" for a in all_company_links]

def get_job_data(url):
  response = requests.get(url)
  soup = BeautifulSoup(response.text, "html.parser")

  # 提取所需的資料
  job_title_elements = soup.find('h1', class_='JobDescriptionLeftColumn_title__4MYvd')
  if job_title_elements:
    job_title = job_title_elements.get_text(strip=True)  
  else:
    job_title = "N/A"
  company_name_elements = soup.find('a', class_='JobDescriptionLeftColumn_name__ABAp9')
  if company_name_elements:
    company_name = company_name_elements.get_text(strip=True)  
  else:
    company_name = "N/A"
  job_description_elements = soup.find('div', class_='JobDescriptionLeftColumn_row__iY44x JobDescriptionLeftColumn_mainContent__VrTGs')
  if job_description_elements:
    job_description = job_description_elements.get_text(strip=True)  
  else:
    job_description = "N/A"
  work_properties_elements = soup.find('div', class_='JobDescriptionRightColumn_row__5rklX')
  if work_properties_elements:
    work_properties = work_properties_elements.get_text(strip=True)  
  else:
    work_properties = "N/A"

  location_element = soup.find('a', class_='CompanyInfoItem_link__cMZwX')
  if location_element:
    location = location_element.get_text(strip=True)  
  else:
    location = "N/A"

  recruit_people_elements = soup.select('div.JobDescriptionRightColumn_jobInfo__9Liba > div:nth-child(3) > span')
  if recruit_people_elements:
    recruit_people = recruit_people_elements[0].text
  else:
    recruit_people = "N/A"

  years_of_experience = soup.select('div.JobDescriptionRightColumn_jobInfo__9Liba > div:nth-child(4) > span')
  if years_of_experience:
    years_of_experience = years_of_experience[0].text
  else:
    years_of_experience = "N/A"

  salary = soup.select('div.JobDescriptionRightColumn_jobInfo__9Liba > div:nth-child(5) > div.JobDescriptionRightColumn_salaryWrapper__Q_8IL > span')
  if salary:
    salary = salary[0].text
  else:
    salary = "N/A"

  company_props = soup.select('div > div.LayoutJob_container__JSBvV > div.AboutBlock_container__LSgHt > div.AboutBlock_textContent__5UUQl > div.AboutBlock_headerRow__WZhTX > div.AboutBlock_companyMeta__O1aEE > div > div:nth-child(1) > span')
  if company_props:
    company_props = company_props[0].text
  else:
    company_props = "N/A"

  work_at_home_elements = soup.select('div.JobDescriptionRightColumn_jobInfo__9Liba > div:nth-child(6) > span')
  if work_at_home_elements:
    work_at_home = work_at_home_elements[0].text
  else:
    work_at_home = "N/A"  


  # 整合資料
  job_data = {
      "title": job_title,
      "company": company_name, 
      "company_props": company_props,
      "description": job_description,
      "properties": work_properties,
      "location": location,
      "recruit_people": recruit_people,
      "years_of_experience": years_of_experience,
      "salary": salary,
      "work_at_home": work_at_home
  }
  return job_data




results = []
for i in links_list:
  datas = get_job_data(i)
  results.append(datas)
  time.sleep(4)

df = pd.DataFrame(results)

df.to_csv('後端工程師.csv', index=False)