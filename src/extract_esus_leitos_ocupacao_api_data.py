from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import csv
import pandas as pd
import asyncio

async def write_results(res_df):
    csv_file = 'ocupacao_leitos_esus.csv'
    res_df.to_csv(csv_file, index=False, quoting = csv.QUOTE_NONNUMERIC)
    print('Data written to: {}'.format(csv_file))

es = Elasticsearch('https://elastic-leitos.saude.gov.br',
                   http_auth=('user-api-leitos', 'aQbLL3ZStaTr38tj'))

index = "leito_ocupacao"

res = es.search(index = index, scroll = '1m', size = 10000)
scroll_id = res['_scroll_id']
print('Total hits: {}'.format(res['hits']['total']['value']))
res_hits = []

while len(res['hits']['hits']):
    res_hits += [x['_source'] for x in res['hits']['hits']]
    print("Hits read: %d" % len(res_hits))
    scroll_id = res['_scroll_id']
    res = es.scroll(scroll_id  = scroll_id, scroll = '1m')

res_hits = pd.DataFrame(res_hits)
asyncio.run(write_results(res_hits))
