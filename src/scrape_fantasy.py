import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

import store_basic

driveroptions = Options()
driveroptions.headless = True
driverService = Service(r'E:\cflpy\geckodriver.exe')
driver = webdriver.Firefox(options=driveroptions, service=driverService)

URL = 'https://fantasy.cfl.ca/login'
driver.get(URL)
driver.implicitly_wait(10)
email = driver.find_element(By.NAME, "email")
password = driver.find_element(By.NAME, "password")
email.send_keys("jonathan.w.hudson@gmail.com")
password.send_keys("11c97f96e97a001ce308dec4699b58b6")
driver.find_elements(By.CLASS_NAME, "form-submit")[0].click()
driver.find_elements(By.CLASS_NAME, "cancel-button")[0].click()
time.sleep(2)
driver.execute_script(
    "document.getElementsByClassName('availableRosterItems-scrollingContainer')[0].style.height = \"10000px\"")
time.sleep(2)
panel = driver.find_elements(By.CLASS_NAME, "availableRosterItems-table")[0]
items = driver.find_elements(By.CLASS_NAME, "cfl-available-roster-items-list-item")
items = [x.text.split("\n") for x in items]
temp = pd.DataFrame(items, columns=["Last", "Pos", "Game", "Time", "Cost"])
temp[["Last", "First"]] = temp['Last'].str.split(", ", 1, expand=True)
temp['Cost'] = temp['Cost'].str[1:].astype(int)
store_df = temp[['Last', 'First', 'Pos', 'Cost']]
store_basic.store_dataframe(store_df, "fantasy", store_basic.IF_EXISTS_REPLACE)
driver.quit()
