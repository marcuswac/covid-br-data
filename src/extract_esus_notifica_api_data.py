from elasticsearch import Elasticsearch, helpers
from joblib import Parallel, delayed
import pandas as pd

def write_row(res_list, csv_file, i, buffer_size):
    res_df = pd.DataFrame(res_list)
    if i <= buffer_size:
        res_df.to_csv(csv_file)
    else:
        res_df.to_csv(csv_file, mode='a', header=False)

def extract_uf(uf):
    es = Elasticsearch('https://elasticsearch-saps.saude.gov.br',
                    http_auth=('user-public-notificacoes', 'Za4qNXdyQNSa9YaA'))

    index = 'desc-notificacoes-esusve-' + uf.lower()
    
    res_scan = helpers.scan(es, index = index, scroll = '30m', size = 10000)
    csv_file_name = 'notificacoes_esusve_{}.csv.gz'.format(uf.lower())
    print("Starting to extract data for {}".format(uf))
    
    buffer_size = 1000000
    res_list = []
    i = 0
    for result in res_scan:
        res_list.append(result['_source'])
        i += 1
        if i % buffer_size == 0:
            write_row(res_list, csv_file_name, i, buffer_size)
            print("Written {} rows for {}".format(i, uf))
            res_list = []
    write_row(res_list, csv_file_name, i, buffer_size)  
    print("Finished to extract data for {}. Total rows: {}".format(uf, i))

ufs = ["SP", "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
       "PB", "PE", "PI", "PR", "RJ", "RN", "RS", "RO", "RR", "SC", "SE", "TO"]

Parallel(n_jobs=4)(delayed(extract_uf)(uf) for uf in ufs)
