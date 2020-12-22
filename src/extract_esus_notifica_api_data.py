from elasticsearch import Elasticsearch, helpers
import csv
from joblib import Parallel, delayed
import gzip

def extract_uf(uf):
    es = Elasticsearch('https://elasticsearch-saps.saude.gov.br',
                    http_auth=('user-public-notificacoes', 'Za4qNXdyQNSa9YaA'))

    index = 'desc-notificacoes-esusve-' + uf.lower()

    fields = ['source_id',
              #'dataSintomas', 'dataRegistro', 'dataAtualizacao',
              'municipio', 'estado', 'municipioNotificacao', 'estadoNotificacao',
              #'comorbidades', 'numeroCnes', 'confirmado',
              'sexo', 'idade', 
              #'febre', 'sinaisGravidade', 'sinaisRespiratorios',
              'dataTeste', 'tipoTeste', 'classificacaoFinal',
              #'sintomas', 'profissionalSaude','profissionalSeguranca', 'cbo', 
              'dataNotificacao', 
              #'outrosSintomas', 'estrangeiro', 'racaCor',
              'evolucaoCaso', 'estadoTeste',
              #'condicoes',
              'dataEncerramento', 'dataInicioSintomas', 'resultadoTeste',
              #'testeSorologico', 'dataTesteSorologico', 'tipoTesteSorologico', 'resultadoTesteSorologicoIgA',
              #'resultadoTesteSorologicoIgG', 'resultadoTesteSorologicoIgM', 'resultadoTesteSorologicoTotais',
              '_updated_at', '@timestamp',
              #'estadoIBGE', 'estadoNotificacaoIBGE',
              'municipioIBGE', 'municipioNotificacaoIBGE']
    res_scan = helpers.scan(es, index = index, _source = fields, scroll = '30m', size = 10000)
    csv_file_name = 'esus-notifica/api/notificacoes_esusve_{}.csv.gz'.format(uf.lower())
    print("Starting to extract data for {}".format(uf))
    
    with gzip.open(csv_file_name, "wt") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()
        writer.writerows((document['_source'] for document in res_scan))
    
    print("Finished to extract data for {}".format(uf))

ufs = ["SP", "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
       "PB", "PE", "PI", "PR", "RJ", "RN", "RS", "RO", "RR", "SC", "SE", "TO"]

Parallel(n_jobs=4)(delayed(extract_uf)(uf) for uf in ufs)
