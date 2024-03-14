from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import boto3
from io import BytesIO

import json
import re

url = 'https://stats.espncricinfo.com/ci/engine/records/team/match_results.html?id=14450;type=tournament'

match_links = []
player_links = []
batting_headers = ["match", "teamInnings", "battingPos", "batsmanName", "runs", "balls", "4s",	"6s", "SR",	"out/not_out", "match_id"]
bowling_headers = ["match",	"bowlingTeam",	"bowlerName",	"overs",	"maiden",	"runs",	"wickets",	"economy",	"0s",	"4s",	"6s",	"wides",	"noBalls",	"match_id"]
player_headers = ["name","team","image","battingStyle","bowlingStyle","playingRole","description"]
batting = []
bowling = []
player = []

def scarpe_matchSummary(url):
    
    global match_links

    service = Service(executable_path=r'/usr/lib/chromium-browser/chromedriver')

    options = Options()
    options.add_argument('--headless')  # Run Chrome in headless mode
    options.add_argument('--no-sandbox')  # Disable sandbox mode
    options.add_argument('--disable-gpu')  # Disable GPU acceleration
    options.add_argument('--disable-dev-shm-usage')  # Disable /dev/shm usage

    with webdriver.Chrome(service=service, options=options) as driver:
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
        driver.quit()

    # Convert table data to JSON string
    json_data = json.dumps(table_data, indent=4)

    # Convert JSON string to bytes
    json_bytes = json_data.encode('utf-8')

    # Create BytesIO object to represent the file
    file_obj = BytesIO(json_bytes)

    # Upload file to S3
    s3 = boto3.client('s3')
    s3.upload_fileobj(file_obj, "t20worldcupdata", "staged/staged_match_summary.json")

def scrape_match(data):
    global batting_headers
    global bowling_headers
    global player_links
    global batting
    global bowling

    service = Service(executable_path=r'/usr/lib/chromium-browser/chromedriver')

    options = Options()
    options.add_argument('--headless')  # Run Chrome in headless mode
    options.add_argument('--no-sandbox')  # Disable sandbox mode
    options.add_argument('--disable-gpu')  # Disable GPU acceleration
    options.add_argument('--disable-dev-shm-usage')  # Disable /dev/shm usage

    with webdriver.Chrome(service=service, options=options) as driver:
        driver.get(data[3])
        wait = WebDriverWait(driver, 10)
        rows = driver.find_elements(By.XPATH, ".//div[contains(concat(' ', normalize-space(@class), ' '), ' ds-rounded-lg ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-mt-2 ')]")
        for row in rows:
            active_team = row.find_element(By.XPATH, ".//span[contains(concat(' ', normalize-space(@class), ' '), ' ds-text-title-xs ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-font-bold ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-capitalize ')]").text
            tables = row.find_elements(By.TAG_NAME, 'table')
            for j, table in enumerate(tables):
                if j < 1:
                    flag = 1
                    bat_pos = 0
                    for i,tr in enumerate(table.find_elements(By.XPATH, ".//div[contains(concat(' ', normalize-space(@class), ' '), ' ds-table-row-compact-bottom ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-border-none ')]")):
                        if tr.text != "":
                            if (i > 0):
                                if (flag):
                                    if "ds-text-tight-s" in tr.get_attribute("class"):
                                        flag = 0
                                    else:
                                        temp = []
                                        bat_pos += 1
                                        for k,td in enumerate(tr.find_elements(By.TAG_NAME, 'td')):
                                            if td.find_elements(By.XPATH, ".//a"):
                                                player_links.append(td.find_element(By.XPATH, ".//a").get_attribute("href"))
                                            if k == 0:
                                                temp.append(re.sub(r'\s*\(.*?\)\s*|[†*]', '', td.text))
                                            elif k == 1:
                                                temp.append("not_out") if td.text == "not out" else temp.append("out")
                                            else:
                                                temp.append(td.text)
                                        print(data[0] + " vs " + data[1],active_team,bat_pos,temp[0],temp[2],temp[3],temp[5],temp[6],temp[7],temp[1],data[2])
                                        batting.append(dict(zip(batting_headers,[data[0] + " vs " + data[1],active_team,bat_pos,temp[0],temp[2],temp[3],temp[5],temp[6],temp[7],temp[1],data[2]])))
                    flag = 1
                else:
                    for i,tr in enumerate(table.find_elements(By.TAG_NAME, 'tr')):
                        if tr.text != "": 
                            if (i > 0):
                                temp = []
                                temp_active = ""
                                for k,td in enumerate(tr.find_elements(By.TAG_NAME, 'td')):
                                    if td.find_elements(By.XPATH, ".//a"):
                                        player_links.append(td.find_element(By.XPATH, ".//a").get_attribute("href"))
                                    if k == 0:
                                        temp.append(re.sub(r'\s*\(.*?\)\s*|[†*]', '', td.text))
                                    else:
                                        temp.append(td.text)
                                if active_team == data[0]: temp_active = data[1] 
                                elif active_team == data[1]: temp_active = data[0] 
                                print(data[0] + " vs " + data[1],temp_active,temp[0],temp[1],temp[2],temp[3],temp[4],temp[5],temp[6],temp[7],temp[8],temp[9],temp[10],data[2])
                                bowling.append(dict(zip(batting_headers, [data[0] + " vs " + data[1],temp_active,temp[0],temp[1],temp[2],temp[3],temp[4],temp[5],temp[6],temp[7],temp[8],temp[9],temp[10],data[2]])))
        driver.quit()

def scrape_playerData(url):
    global player
    global player_headers

    service = Service(executable_path=r'/usr/lib/chromium-browser/chromedriver')

    options = Options()
    options.add_argument('--headless')  # Run Chrome in headless mode
    options.add_argument('--no-sandbox')  # Disable sandbox mode
    options.add_argument('--disable-gpu')  # Disable GPU acceleration
    options.add_argument('--disable-dev-shm-usage')  # Disable /dev/shm usage

    with webdriver.Chrome(service=service, options=options) as driver:
        driver.get(url)
        wait = WebDriverWait(driver, 10)

        name = ""
        country = ""
        image = ""
        battingStyle = ""
        bowlingStyle = ""
        playingRole = ""
        description = ""

        name = driver.find_element(By.XPATH, ".//h1[contains(concat(' ', normalize-space(@class), ' '), ' ds-text-title-l ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-font-bold ')]").text

        country = driver.find_element(By.XPATH, ".//span[contains(concat(' ', normalize-space(@class), ' '), ' ds-text-comfortable-s ')]").text

        if driver.find_elements(By.XPATH, ".//div[contains(concat(' ', normalize-space(@class), ' '), ' ci-player-bio-content ') ]"):
            for des in driver.find_elements(By.XPATH, ".//div[contains(concat(' ', normalize-space(@class), ' '), ' ci-player-bio-content ') ]"):
                description = des.find_element(By.TAG_NAME, 'p').text
        
        p = driver.find_elements(By.XPATH, ".//div[contains(concat(' ', normalize-space(@class), ' '), ' ds-grid ') and contains(concat(' ', normalize-space(@class), ' '), ' lg:ds-grid-cols-3 ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-grid-cols-2 ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-gap-4 ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-mb-8 ')]")

        for ps in p:
            cols = ps.find_elements(By.TAG_NAME, 'div')
            for col in cols:
                if col.find_elements(By.XPATH, ".//p[contains(concat(' ', normalize-space(@class), ' '), ' ds-text-tight-m ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-font-regular ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-uppercase ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-text-typo-mid3 ')]"):
                    text = col.find_element(By.XPATH, ".//p[contains(concat(' ', normalize-space(@class), ' '), ' ds-text-tight-m ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-font-regular ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-uppercase ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-text-typo-mid3 ')]").text
                    content = col.find_element(By.XPATH, ".//span[contains(concat(' ', normalize-space(@class), ' '), ' ds-text-title-s ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-font-bold ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-text-typo ')]").text
                    if (text == "BATTING STYLE"):
                        battingStyle = content
                    elif (text == "BOWLING STYLE"):
                        bowlingStyle = content
                    elif (text == "PLAYING ROLE"):
                        playingRole = content


        if driver.find_elements(By.XPATH, ".//div[contains(concat(' ', normalize-space(@class), ' '), ' ds-ml-auto ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-w-48 ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-h-48 ')]"):
            for img in driver.find_elements(By.XPATH, ".//div[contains(concat(' ', normalize-space(@class), ' '), ' ds-ml-auto ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-w-48 ') and contains(concat(' ', normalize-space(@class), ' '), ' ds-h-48 ')]"):
                tem = "//img[@alt and contains(concat(' ', normalize-space(@alt), ' '), \"{}\")]".format(name)
                image = img.find_element(By.XPATH, tem).get_attribute("src")
        
        print(name, country, image, battingStyle, bowlingStyle, playingRole, description)
        player.append(dict(zip(batting_headers,[name, country, image, battingStyle, bowlingStyle, playingRole, description])))
        
        driver.quit()


print("Match Summary Started")
scarpe_matchSummary(url)
print("Match Summary Completed")


print("Match Detail Started")
for i, data in enumerate(match_links):
        scrape_match(data)

json_data = json.dumps(batting, indent=4)

json_bytes = json_data.encode('utf-8')

file_obj = BytesIO(json_bytes)

s3 = boto3.client('s3')
s3.upload_fileobj(file_obj, "t20worldcupdata", "staged/staged_match_batting_summary.json")

json_data = json.dumps(bowling, indent=4)

json_bytes = json_data.encode('utf-8')

file_obj = BytesIO(json_bytes)

s3 = boto3.client('s3')
s3.upload_fileobj(file_obj, "t20worldcupdata", "staged/staged_match_bowling_summary.json")
print("Match Detail Completed")

print("Player Data Started")
player_links = list(set(player_links))

for link in  player_links:
    scrape_playerData(link)

json_data = json.dumps(player, indent=4)

json_bytes = json_data.encode('utf-8')

file_obj = BytesIO(json_bytes)

s3 = boto3.client('s3')
s3.upload_fileobj(file_obj, "t20worldcupdata", "staged/staged_players.json")

print("Player Data Completed")