from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

import json

url = 'https://stats.espncricinfo.com/ci/engine/records/team/match_results.html?id=14450;type=tournament'
match_summary_path = 'dataset_json/match_summary.json'

def scarpe_matchSummary(url, filename):
    
    global match_links

    service = Service(executable_path=r'/usr/lib/chromium-browser/chromedriver')

    options = Options()
    options.add_argument('--headless')  # Run Chrome in headless mode
    options.add_argument('--no-sandbox')  # Disable sandbox mode
    options.add_argument('--disable-gpu')  # Disable GPU acceleration
    options.add_argument('--disable-dev-shm-usage')  # Disable /dev/shm usage

    with webdriver.Chrome(options=options) as driver:
        driver.get(url)
        WebDriverWait(driver, 10).until( lambda x: x.find_element(By.TAG_NAME, 'table'))
        headers = [header.text for header in driver.find_elements(By.TAG_NAME, 'td')]
        table_data = []
        for row in driver.find_elements(By.TAG_NAME, 'tr')[1:]:
            cols = row.find_elements(By.TAG_NAME, 'td')
            row_data = [data.text for data in row.find_elements(By.TAG_NAME, 'td')]
            table_data.append(dict(zip(headers, row_data)))
            if cols[6].find_elements(By.XPATH, ".//a"):
                match_links.append([cols[0].text, cols[1].text, cols[6].text, cols[6].find_element(By.XPATH, ".//a").get_attribute("href")])

    with open(filename, 'w') as f:
        json.dump(table_data, f, indent=4)

scarpe_matchSummary(url, match_summary_path)