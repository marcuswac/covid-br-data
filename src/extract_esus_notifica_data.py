from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import csv
import pandas as pd

es = Elasticsearch('https://elasticsearch-saps.saude.gov.br',
                   http_auth=('user-public-notificacoes', 'Za4qNXdyQNSa9YaA'))

query = {
	"sort" : [
        { "dataAtualizacao" : {"order" : "desc"}}
       ]
}

res = es.search(index = 'desc-notificacoes-esusve-*', body = query,
                scroll = '1m', size = 10000)
scroll_id = res['_scroll_id']

print("Got %d Hits Total" % res['hits']['total']['value'])
res_hits = []

while len(res['hits']['hits']):
    res_hits += [x['_source'] for x in res['hits']['hits']]
    print("Hits read: %d" % len(res_hits))
    scroll_id = res['_scroll_id']
    res = es.scroll(scroll_id  = scroll_id, scroll = '1m')

res_df = pd.DataFrame(res_hits)
res_df.to_csv('notificacoes_esusve.csv.gz', index=False)
