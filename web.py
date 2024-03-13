url = 'https://stats.espncricinfo.com/ci/engine/records/team/match_results.html?id=14450;type=tournament'

import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import re

# To Store All The Match Links
match_links = []
# To Store All The Player Links
player_links = []

driver = webdriver.Chrome()

driver.get(url)

wait = WebDriverWait(driver, 10)

rows = driver.find_elements(By.TAG_NAME, 'tr')

# with open('dataset/match_summary1.csv', mode='w', newline='', encoding='utf-8') as file:
#     writer = csv.writer(file)
for row in rows:
    cols = row.find_elements(By.TAG_NAME, 'td')
    if cols:
        data = [col.text for col in cols]
        if cols[6].find_elements(By.XPATH, ".//a"):
            match_links.append([cols[0].text,cols[1].text,cols[6].text,cols[6].find_element(By.XPATH, ".//a").get_attribute("href")])
        # writer.writerow(data)
print(match_links)

driver.quit()