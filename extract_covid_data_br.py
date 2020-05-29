from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import glob
import pandas as pd
import sys
import time, os

def download_ms_data(download_dir):
    print("Downloading file to", download_dir)
    xlsx_files = glob.glob(os.path.join(download_dir, "HIST_PAINEL_COVIDBR_*.xlsx"))
    n_before = len(xlsx_files)

    # To prevent download dialog
    profile = webdriver.FirefoxProfile()
    profile.set_preference('browser.download.folderList', 2) # custom location
    profile.set_preference('browser.download.manager.showWhenStarting', True)
    profile.set_preference('browser.download.dir', download_dir)
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk',
                           'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    driver = webdriver.Firefox(profile)
    driver.get("https://covid.saude.gov.br")

    time.sleep(10)
    button_xpath = "/html/body/app-root/ion-app/ion-router-outlet/app-home/ion-content/div[1]/div[2]/ion-button"
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, button_xpath)))
    element = WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, button_xpath)))

    element.click()
    
    # Wait for file to be created
    while len(xlsx_files) == n_before:
        print("Waiting +10 seconds to finish downloading...")
        time.sleep(10)
        xlsx_files = glob.glob(os.path.join(download_dir, "HIST_PAINEL_COVIDBR_*.xlsx"))

    xlsx_files.sort(key=os.path.getmtime, reverse=True)
    downloaded_file = xlsx_files[0]
    print("File downloaded:", downloaded_file)
    
    driver.quit()
    return(downloaded_file)

def filter_country(df):
    df_country = df[df.estado.isnull() & df.codmun.isnull()]
    return(df_country)

def filter_states(df):
    df_states = df[df.estado.notnull() & df.codmun.isnull()]
    return(df_states)

def filter_cities(df):
    df_cities = df[df.municipio.notnull()]
    return(df_cities)

def export_csvs(excel_file, csv_prefix):
    df_complete = pd.read_excel(excel_file)
    csv_output = csv_prefix + "-complete.csv"
    df_complete.to_csv(csv_output, index=False)
    print("CSV complete exported:", csv_output)

    df = filter_country(df_complete)
    csv_output = csv_prefix + "-country.csv"
    df.to_csv(csv_output, index=False)
    print("CSV for country exported:", csv_output)
    
    df = filter_states(df_complete)
    csv_output = csv_prefix + "-states.csv"
    df.to_csv(csv_output, index=False)
    print("CSV by state exported:", csv_output)

    df = filter_cities(df_complete)
    csv_output = csv_prefix + "-cities.csv"
    df.to_csv(csv_output, index=False)
    print("CSV by city exported:", csv_output)
    
def main(args):
    download_rel_dir = args[0] if len(args) > 0 else "data"
    download_dir = os.path.abspath(download_rel_dir)
    csv_filename_prefix = args[1] if len(args) > 1 else 'covid-br-ms'
    csv_output_prefix = os.path.join(download_dir, csv_filename_prefix)

    downloaded_file = download_ms_data(download_dir)
    export_csvs(downloaded_file, csv_output_prefix)

if __name__ == '__main__':
    main(sys.argv[1:])
