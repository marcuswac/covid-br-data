from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import csv
import pandas as pd
import asyncio

async def write_results(res_df, uf):
    csv_file = 'notificacoes_esusve_{}.csv.gz'.format(uf.lower())
    res_df.to_csv(csv_file, index=False, quoting = csv.QUOTE_NONNUMERIC)
    print('Data written to: {}'.format(csv_file))

ufs = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
       "PB", "PE", "PI", "PR", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

for uf in ufs:
#for uf in ["complete"]:
    es = Elasticsearch('https://elasticsearch-saps.saude.gov.br',
                    http_auth=('user-public-notificacoes', 'Za4qNXdyQNSa9YaA'))

    index = 'desc-notificacoes-esusve-' + uf.lower()
    #index = 'desc-notificacoes-esusve-*'

    res = es.search(index = index, scroll = '1m', size = 10000)
    scroll_id = res['_scroll_id']

    print('Data for {}. Total hits: {}'.format(uf, res['hits']['total']['value']))
    res_hits = []

    while len(res['hits']['hits']):
        res_hits += [x['_source'] for x in res['hits']['hits']]
        print("Hits read: %d" % len(res_hits))
        scroll_id = res['_scroll_id']
        res = es.scroll(scroll_id  = scroll_id, scroll = '1m')

    res_hits = pd.DataFrame(res_hits)
    asyncio.run(write_results(res_hits, uf))
