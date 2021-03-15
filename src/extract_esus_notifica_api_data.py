from elasticsearch import Elasticsearch, helpers
from joblib import Parallel, delayed
import pandas as pd

ufs = {
    'RO':11, 'AC':12, 'AM':13, 'RR':14, 'PA':15, 'AP':16, 'TO':17,
    'MA':21, 'PI':22, 'CE':23, 'RN':24, 'PB':25, 'PE':26, 'AL':27, 'SE':28, 'BA':29,
    'MG':31, 'ES':32, 'RJ':33, 'SP':35, 
    'PR':41, 'SC':42, 'RS':43, 
    'MS':50, 'MT':51, 'GO':52, 'DF':53
}

def write_row(res_list, csv_file, i, buffer_size):
    res_df = pd.DataFrame(res_list)
    if i <= buffer_size:
        res_df.to_csv(csv_file)
    else:
        res_df.to_csv(csv_file, mode='a', header=False)

def extract_uf(uf):
    es = Elasticsearch('https://elasticsearch-saps.saude.gov.br',
                    http_auth=('user-public-notificacoes', 'Za4qNXdyQNSa9YaA'))

    #index = 'desc-notificacoes-esusve-' + uf.lower()
    index = 'desc-notificacoes-esusve-*'
    query = {"query": { "match": { "estadoNotificacaoIBGE": ufs[uf]} } }
    
    res_scan = helpers.scan(es, query = query, index = index, scroll = '30m', size = 10000)
    print("Starting to extract data for {}".format(uf))
    
    buffer_size = 1000000
    res_list = []
    nrows = 0
    i = 1
    for result in res_scan:
        res_list.append(result['_source'])
        nrows += 1
        if nrows % buffer_size == 0:
            csv_file_name = 'notificacoes_esusve_{}-{}.csv.gz'.format(uf.lower(), i)
            write_row(res_list, csv_file_name, 0, buffer_size)
            print("Written {} rows for {}".format(nrows, uf))
            res_list = []
            i += 1
    csv_file_name = 'notificacoes_esusve_{}-{}.csv.gz'.format(uf.lower(), i)
    write_row(res_list, csv_file_name, 0, buffer_size)  
    print("Finished to extract data for {}. Total rows: {}".format(uf, nrows))

Parallel(n_jobs=4)(delayed(extract_uf)(uf) for uf in ufs.keys())
