from datetime import datetime as dt, timedelta
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import requests
import json

data_at = dt.today().strftime('%Y-%m-%d')
data_ini = '2021-01-01'  # Data de inicio do processo
data_fim = data_ini
cont = 1
'''
Intervalo de dias a ser baixado, exemplo: caso for 15, o script irá baixar os dados 
de 15 em 15 dias (e adicionar no banco) desde a data de inicio até o a data atual
'''
dias = 15


def dump(df) -> None:
    '''Função que insere dados do dataframe no banco '''
    while True:
        try:
            # TODO dados do banco (apagar sempre)
            name = ''
            user = ''
            password = ''
            host = ''
            port = ''

            link = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}'

            engine = create_engine(link)
            df.to_sql(
                'imunizacaopb',
                con=engine,
                if_exists='append',
                index=False
            )
            con = engine.raw_connection()
            con.commit()
            con.close()

        except Exception as e:
            raise Exception(e)
        else:
            break


while True:
    data_inicio = data_ini+'T00:00:00.000Z'
    data_fim = (dt.strptime(data_ini, '%Y-%m-%d') +
                timedelta(days=14)).strftime(data_fim, '%Y-%m-%d')
    if data_fim < data_at:
        data_fim += 'T23:59:59.999Z'
    else:
        data_fim = data_at+'T00:00:00.000Z'
        cont += 1

    print(data_inicio)
    print(data_fim)

    # TODO dados da api, login e senha (apagar sempre)
    url = 'https://imunizacao-es.saude.gov.br/_search?scroll=1m'
    login = ''
    pwd = ''
    headers = {'Content-Type': 'application/json'}
    body = {
        "size": 10000,
        "query": {
            "bool": {
                "filter": [{
                    "range": {
                        "data_importacao_datalake": {
                            "gte": data_inicio,
                            "lte": data_fim
                        }
                    }
                }]
            }
        }
    }

    response = requests.request(
        "POST",
        url,
        data=json.dumps(body),
        auth=HTTPBasicAuth(login, pwd),
        headers=headers
    )

    result = response.json()
    # número de casos no periodo solicitado
    casos = result['hits']['total']['value']
    print('numero de casos: ', casos, type(casos))
    scroll_id = result['_scroll_id']
    result = result['hits']['hits']
    quant = 1  # Contador de solicitações, apenas para orientação
    print(quant)
    print(len(result))
    df = pd.json_normalize(document['_source'] for document in result)
    while result:
        '''Faz a função de scroll, ou seja, vai passando de página em página na resposta e inserindo os dados no dataframe'''
        url = 'https://imunizacao-es.saude.gov.br/_search/scroll'
        body = {
            "scroll_id": scroll_id,
            "scroll": "1m"
        }

        response = requests.request(
            "POST",
            url, data=json.dumps(body),
            auth=HTTPBasicAuth(login, pwd),
            headers=headers
        )

        result = response.json()
        try:
            scroll_id = result['_scroll_id']
            result = result['hits']['hits']
        except KeyError:
            print(result)
            exit()
        else:
            print(quant)
            print(len(result))
            df = df.append(pd.json_normalize(
                document['_source'] for document in result))
        quant += 1

    print('casos baixados: ', df.shape[0])
    '''Checa se os dados baixados têm a mesma quantidade dos disponibilizados'''
    if df.shape[0] == casos:
        if df.shape[0] > 0:
            dump(df)
        if cont == 2:
            break
        data_ini = dt.strptime(data_ini, '%Y-%m-%d')+timedelta(days=15)
        data_ini = dt.strftime(data_ini, '%Y-%m-%d')
