import json
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import requests

def download_ms_data(download_dir):
    print("Downloading file to", download_dir)
    url = "https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/"
    headers = {
        "accept": "application/json, text/plain, /",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "x-parse-application-id": "unAFkcaNDeXajurGB7LChj8SgQYS2ptm",
    }

    request = requests.get(url + "PortalGeral", headers=headers)
    content = request.content.decode("utf8")
    data = json.loads(content)["results"][0]

    #zip_file = data["arquivo"]["url"]
    #df = pd.read_csv(zip_file, compression = 'zip', sep = ';')
    csv_file = data["arquivo"]["url"]
    df = pd.read_csv(csv_file, sep = ';')

    today_date_str = "".join(str(datetime.now().date()).split("-"))
    downloaded_file = os.path.join(download_dir, "covid-br-ms-complete.csv.gz")
    df.to_csv(downloaded_file, index=False, compression = 'gzip')
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

def export_csvs(csv_file, csv_prefix):
    df_complete = pd.read_csv(csv_file)

    df = filter_country(df_complete)
    csv_file = csv_prefix + "-country.csv.gz"
    df.to_csv(csv_file, index=False, compression = 'gzip')
    print("CSV for country exported:", csv_file)
    
    df = filter_states(df_complete)
    csv_file = csv_prefix + "-states.csv.gz"
    df.to_csv(csv_file, index=False, compression = 'gzip')
    print("CSV by state exported:", csv_file)

    df = filter_cities(df_complete)
    csv_file = csv_prefix + "-cities.csv.gz"
    df.to_csv(csv_file, index=False, compression = 'gzip')
    print("CSV by city exported:", csv_file)
    
def main(args):
    download_rel_dir = args[0] if len(args) > 0 else ""
    download_dir = os.path.abspath(download_rel_dir)
    csv_filename_prefix = args[1] if len(args) > 1 else "covid-br-ms"
    csv_output_prefix = os.path.join(download_dir, csv_filename_prefix)

    downloaded_file = download_ms_data(download_dir)
    export_csvs(downloaded_file, csv_output_prefix)

if __name__ == '__main__':
    main(sys.argv[1:])
