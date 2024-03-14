from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

url = 'https://stats.espncricinfo.com/ci/engine/records/team/match_results.html?id=14450;type=tournament'

service = Service(executable_path=r'/usr/lib/chromium-browser/chromedriver')

options = Options()
options.add_argument('--headless')  # Run Chrome in headless mode
options.add_argument('--no-sandbox')  # Disable sandbox mode
options.add_argument('--disable-gpu')  # Disable GPU acceleration
options.add_argument('--disable-dev-shm-usage')  # Disable /dev/shm usage

driver = webdriver.Chrome(service=service, options=options)

# To Store All The Match Links
match_links = []
# To Store All The Player Links
player_links = []

driver.get(url)

wait = WebDriverWait(driver, 10)

rows = driver.find_elements(By.TAG_NAME, 'tr')

for i, row in enumerate(rows):
    cols = row.find_elements(By.TAG_NAME, 'td')
    if cols:
        data = [col.text for col in cols]
        if cols[6].find_elements(By.XPATH, ".//a"):
            match_links.append([cols[0].text, cols[1].text, cols[6].text, cols[6].find_element(By.XPATH, ".//a").get_attribute("href")])
            print(i)
            print([cols[0].text, cols[1].text, cols[6].text, cols[6].find_element(By.XPATH, ".//a").get_attribute("href")])

driver.quit()