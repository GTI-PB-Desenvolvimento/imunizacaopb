import imp
from datetime import datetime as dt, timedelta
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import requests
import json
import os

'''
Data de inicio = um dia antes (para mudar, só aumentar ou diminuir o valor no timedelta)
Data fim = data atual
Para mudar os horários basta alterar os valores no fim das datas (T05:00:00.000Z) !!! um milésimo pode resultar em dados duplicados, então evitar que os horários de inicio e fim se batam, Exemplo: 05:00:00.001 até 05:00:00.000
'''
data_inicio = (dt.today()-timedelta(1)).strftime('%Y-%m-%d')+'T05:00:00.00Z'
data_fim = dt.today().strftime('%Y-%m-%d')+'T04:59:59.999Z'


def dump(df) -> None:
    '''Função que insere dados do dataframe no banco '''
    global data_fim, data_inicio
    erro = 0
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


erro = 0  # Contador de erros (máximo 5)
print(data_inicio + 6*'-' + data_fim)

while True:
    try:
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
        else:
            continue
    except Exception as e:
        '''Caso aconteça algo de errado, volta a tentar por no máximo 5 vezes, chegando neste limite, fecha o script'''
        erro += 1
        os.system(
            f'echo "{data_inicio}{5*"-"}{data_fim} - Erro: {e}" >> Log.txt')
        if erro >= 5:
            exit()
        continue
    else:
        os.system(
            f'echo "{data_inicio}{3*"-"}{data_fim} - Baixado: {df.shape[0]} casos de {casos}" >> Log.txt')
        break
