'''Projeto de extração e manipulação de dados pluviométricos para o estados de Pernambuco'''
#Bibliotecas
import requests
import numpy as np
import pandas as pd
import json
from sqlalchemy import create_engine

#Conectando API
url = "http://dados.apac.pe.gov.br:41120/cemaden/"
response = requests.get(url)
dados = response.json()

#Organizando dados em um Dataframe
df = pd.DataFrame(dados)
df_normalizado = df['Dados_completos'].apply(json.loads).apply(pd.Series)
df_final = pd.concat([df.drop(columns=['Dados_completos']), df_normalizado], axis=1)

#Filtrando os dados que serão utilizados
colunas = df_final.columns
df_final.drop(columns=['dataHora',
       'intensidade_precipitacao', 'Precipitacao_acumulada',
       'Precipitacao_nao_registrada', 'umidade_solo_nivel1',
       'umidade_solo_nivel1_max', 'umidade_solo_nivel1_min',
       'umidade_solo_nivel2', 'umidade_solo_nivel2_max',
       'umidade_solo_nivel2_min', 'umidade_solo_nivel3',
       'umidade_solo_nivel3_max', 'umidade_solo_nivel3_min',
       'umidade_solo_nivel4', 'umidade_solo_nivel4_max',
       'umidade_solo_nivel4_min', 'umidade_solo_nivel5',
       'umidade_solo_nivel5_max', 'umidade_solo_nivel5_min',
       'umidade_solo_nivel6', 'umidade_solo_nivel6_max',
       'umidade_solo_nivel6_min'],inplace=True)

#Correção de tipo data
df_final['Data-hora'] = pd.to_datetime(df_final['Data-hora'])

#Pivotando dados
df_pivotado = df_final.pivot_table(index='Data-hora',
                             columns='Estação',
                             values='chuva',
                             aggfunc='sum')

#Removendo colunas duplicadas
normalized_cols = df_pivotado.columns.str.lower().str.strip()

df_pivotado = df_pivotado.loc[:, ~normalized_cols.duplicated()]

#Depositando em um banco de dados
engine = create_engine('sqlite:///arquivo.db')
df_pivotado.to_sql('Prec_PE', engine, if_exists='replace', index=True)


