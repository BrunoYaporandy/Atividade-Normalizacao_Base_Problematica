# ##############################################################
# ########### Bruno Yaporandy - Atividade 18 ###################
# ##############################################################

#   Realize a extração da base de dados de dados em anexo 
#   Popule o mongo atlas com a mesma 
#   Extraia novamente a base diretamente do Atlas e realize as normalizações necessárias
#   Utilize somente python e pandas para as normalizações e limpeza
#   Não realize drop de nenhuma linha 
#   Construa 3 insighits relevantes da base 
#   Utilize apenas matplotlib ou o próprio pandas para plotar os gráficos
#   e carregue o dataframe tratado em um banco relacional em nuvem dentro da GCP


import pandas as pd
from modules.connector_postgres import Connector_postgres
from modules.connector_mongodb import Connector_mongodb
from pymongo import MongoClient
from matplotlib import pyplot as plt
import datetime

if __name__ == "__main__":
    try:
        print("Conectando ao banco de dados Mongo Atlas...")
        client = MongoClient('mongodb+srv://brunoyaporandy-atv18:8pBnx7kDAuI1jsXG@cluster0.gcl6m.mongodb.net/atv18?retryWrites=true&w=majority')
        print("Conexão com o Mongo Atlas concluida.")
        
        print("Criando um DataFrame para realizar as normalizações necessárias...")
        db = client["atv18"]
        collection = db["ocorrencias"]
        
        df_original = pd.DataFrame(collection.find())
        df = df_original
        print("DataFrame criado com sucesso!")
        
        print("Iniciando a normalização dos dados:")
        
        #Quais são as colunas do meu DataFrame?
        df_columns = df.columns.values.tolist()
        #print(df_columns)
                
        print("Substituindo valores NaN por NULO")
        df = df.fillna("NULO")
        
        #Percebi que haviam valores *, **, *** que estavam fora da normalização das colunas.
        #Tomei a decisão de substitui-los por "NULO"
        print("Substituindo *, **, *** por NULO")
        df = df.replace("*", "NULO")
        df = df.replace("**", "NULO")
        df = df.replace("***", "NULO")
        
        df['ocorrencia_cidade'] = df['ocorrencia_cidade'].str.replace("'", " ")
        
        
        
        #####TRATANDO A COLUNA 'ocorrencia_latitude':######
        print("Tratando a coluna 'ocorrencia_latitude'..")
        #Percebi que na coluna "ocorrencia_latitude" havia caracteres
        # que não condiziam com a normalização da coluna (\t), fiz o tratamento:
        df['ocorrencia_latitude'] = df['ocorrencia_latitude'].str.replace('\\', '')
        df['ocorrencia_latitude'] = df['ocorrencia_latitude'].str.replace('t', '')
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'ocorrencia_longitude':#####
        print("Tratando a coluna 'ocorrencia_longitude'..")
        #Percebi que na coluna "ocorrencia_longitude" havia caracteres
        # que não condiziam com a normalização da coluna (\t), fiz o tratamento:
        df['ocorrencia_longitude'] = df['ocorrencia_longitude'].str.replace('\\', '')
        df['ocorrencia_longitude'] = df['ocorrencia_longitude'].str.replace('t', '')
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'ocorrencia_aerodromo':#####
        print("Tratando a coluna 'ocorrencia_aerodromo'..")
        #Verificando o estado da coluna:
        #print(df['ocorrencia_aerodromo'].unique())
        #realizando o tratamento:
        df['ocorrencia_aerodromo'] = df['ocorrencia_aerodromo'].replace("****", "NULO")
        df['ocorrencia_aerodromo'] = df['ocorrencia_aerodromo'].replace('*****', 'NULO')
        df['ocorrencia_aerodromo'] = df['ocorrencia_aerodromo'].replace('###!', 'NULO')
        df['ocorrencia_aerodromo'] = df['ocorrencia_aerodromo'].replace('####', 'NULO')
        df['ocorrencia_aerodromo'] = df['ocorrencia_aerodromo'].str.replace('9', '')
        print('Coluna tratada com sucesso.')        

        #####TRATANDO A COLUNA 'ocorrencia_dia':#####
        print("Tratando a coluna 'ocorrencia_dia'..")
        #É uma coluna que recebe valores de data, verificando se o tipo de dados está de acordo:
        #print(df['ocorrencia_dia'].info())
        df['ocorrencia_dia'] = pd.to_datetime(df['ocorrencia_dia'], format="%d/%m/%Y")
        df['ocorrencia_dia'] = df['ocorrencia_dia'].dt.strftime('%d-%m-%Y')
        #Coluna com valores datetime64.
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'ocorrencia_hora':#####
        print("Tratando a coluna 'ocorrencia_hora'..")
        #Verificando informações da coluna
        #print(df['ocorrencia_hora'].head())
        #Transformando para to_datetime
        #df['ocorrencia_hora'] = pd.to_datetime(df['ocorrencia_hora'], format="%H:%M:%S")
        #df['ocorrencia_hora'] = df['ocorrencia_hora'].dt.strftime('%H:%M:%S')
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'investigacao_aeronave_liberada':#####
        print("Tratando a coluna 'investigacao_aeronave_liberada'..")
        #Verificando o estado da coluna:
        #print(df['investigacao_aeronave_liberada'].unique())
        #Transformando "Null" para "Nulo" (Para concordância da tabela)
        df['ocorrencia_aerodromo'] = df['ocorrencia_aerodromo'].replace('NULL', 'NULO')
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'investigacao_status':#####
        print("Tratando a coluna 'investigacao_status'..")
        #Verificando o estado da coluna:
        #print(df['investigacao_status'].unique())
        #Transformando "Null" para "Nulo" (Para concordância da tabela)
        df['investigacao_status'] = df['investigacao_status'].replace('NULL', 'NULO')
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'divulgacao_relatorio_numero':#####
        print("Tratando a coluna 'divulgacao_relatorio_numero'..")
        #Verificando o estado da coluna:
        #print(df['divulgacao_relatorio_numero'].unique())
        #Transformando "Null" para "Nulo" (Para concordância da tabela)
        df['divulgacao_relatorio_numero'] = df['divulgacao_relatorio_numero'].replace('NULL', 'NULO')
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'divulgacao_relatorio_publicado':#####
        print("Tratando a coluna 'divulgacao_relatorio_publicado'..")
        #Verificando o estado da coluna:
        #print(df['divulgacao_relatorio_publicado'].unique())
        #Coluna está OK.
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'total_recomendacoes':#####
        print("Tratando a coluna 'total_recomendacoes'..")
        #Verificando o estado da coluna:
        #print(df['total_recomendacoes'].unique())
        #Coluna está OK.
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'total_aeronaves_envolvidas':#####
        print("Tratando a coluna 'total_aeronaves_envolvidas'..")
        #Verificando o estado da coluna:
        #print(df['total_aeronaves_envolvidas'].unique())
        #Coluna está OK.
        print('Coluna tratada com sucesso.')
        
        #####TRATANDO A COLUNA 'ocorrencia_saida_pista':#####
        print("Tratando a coluna 'ocorrencia_saida_pista'..")
        #Verificando o estado da coluna:
        #print(df['ocorrencia_saida_pista'].unique())
        #Coluna está OK.
        print('Coluna tratada com sucesso.')
        
        print('TODAS AS COLUNAS FORAM TRATADA COM SUCESSO.')
        
        
    
        # insights = input("Qual insight você deseja visualizar?\n 1- Estado com mais ocorrencia? \n 2-Cidade com mais ocorrencia \n 3-Os relatorios são publicados? ")
        
        # if insights == "1":
        #     print("Quais são os estados com mais ocorrencias?")
        #     #print(df['ocorrencia_uf'].value_counts())
        #     total_ocorrencia_uf = df['ocorrencia_uf'].value_counts()
        #     cinco_maiores_uf =total_ocorrencia_uf[0:5]
        #     plt.plot(cinco_maiores_uf)
        #     plt.xlabel('ESTADO')
        #     plt.ylabel('QUANTIDADE DE OCORRENCIAS')
        #     plt.grid(True)
        #     plt.show()
        
        # elif insights == "2":
        #     print("Quais as cidades com mais ocorrencias?")
        #     total_ocorrencia_cidade = df['ocorrencia_cidade'].value_counts()
        #     cinco_maiores_cidades = total_ocorrencia_cidade[0:5]
        #     plt.plot(cinco_maiores_cidades)
        #     plt.xlabel('CIDADE')
        #     plt.ylabel('QUANTIDADE DE OCORRENCIAS')
        #     plt.grid(True)
        #     plt.show()
        
        # elif insights == "3":
        #     print("Os relatórios são publicados?")
        #     relatorios_publicados = df['divulgacao_relatorio_publicado'].value_counts()
        #     plt.plot(relatorios_publicados)
        #     plt.xlabel('CIDADE')
        #     plt.ylabel('PUBLICAÇÃO')
        #     plt.grid(True)
        #     plt.show()
            
        
        print("COMEÇANDO..")
        banco=Connector_postgres("35.239.148.92","atv18","postgres","meupassword123")
        #df.to_excel(r'C:\\Users\\Yaporandy\\Desktop\\SOULCODE\\atividades\\Atv18\\data\\testeocorrencias.xlsx')
        print("conectado..")       
        banco.inserir(f'''
                      CREATE TABLE IF NOT EXISTS ocorrencias (
                            
                            _id varchar(50),
                            codigo_ocorrencia varchar(50),
                            codigo_ocorrencia1 varchar(50),
                            codigo_ocorrencia2 varchar(50),
                            codigo_ocorrencia3 varchar(50),
                            codigo_ocorrencia4 varchar(50),
                            ocorrencia_classificacao varchar(50),
                            ocorrencia_cidade varchar(50),
                            ocorrencia_uf varchar(50),
                            ocorrencia_pais varchar(50),
                            ocorrencia_aerodromo varchar(50),
                            ocorrencia_dia varchar(50),
                            ocorrencia_hora varchar(50),
                            investigacao_aeronave_liberada varchar(50),
                            investigacao_status varchar(50),
                            divulgacao_relatorio_numero varchar(50),
                            divulgacao_relatorio_publicado varchar(50),
                            divulgacao_dia_publicacao varchar(50),
                            total_recomendacoes varchar(50),
                            total_aeronaves_envolvidas varchar(50),
                            ocorrencia_saida_pista varchar(50),
                            ocorrencia_latitude varchar(50),
                            ocorrencia_longitude varchar(50)
                          
                        );
                        '''
                      )
        
     
        for i,x in df.iterrows():
            banco.inserir(f"INSERT INTO ocorrencias (_id,codigo_ocorrencia,codigo_ocorrencia1,codigo_ocorrencia2,codigo_ocorrencia3,codigo_ocorrencia4,ocorrencia_classificacao,ocorrencia_cidade,ocorrencia_uf,ocorrencia_pais,ocorrencia_aerodromo,ocorrencia_dia,ocorrencia_hora,investigacao_aeronave_liberada,investigacao_status,divulgacao_relatorio_numero,divulgacao_relatorio_publicado,divulgacao_dia_publicacao,total_recomendacoes,total_aeronaves_envolvidas,ocorrencia_saida_pista,ocorrencia_latitude,ocorrencia_longitude) VALUES ('{df['_id'][i]}', '{df['codigo_ocorrencia'][i]}', '{df['codigo_ocorrencia1'][i]}', '{df['codigo_ocorrencia2'][i]}' , '{df['codigo_ocorrencia3'][i]}', '{df['codigo_ocorrencia4'][i]}', '{df['ocorrencia_classificacao'][i]}', '{df['ocorrencia_cidade'][i]}', '{df['ocorrencia_uf'][i]}', '{df['ocorrencia_pais'][i]}', '{df['ocorrencia_aerodromo'][i]}', '{df['ocorrencia_dia'][i]}', '{df['ocorrencia_hora'][i]}', '{df['investigacao_aeronave_liberada'][i]}', '{df['investigacao_status'][i]}', '{df['divulgacao_relatorio_numero'][i]}', '{df['divulgacao_relatorio_publicado'][i]}', '{df['divulgacao_dia_publicacao'][i]}', '{df['total_recomendacoes'][i]}', '{df['total_aeronaves_envolvidas'][i]}', '{df['ocorrencia_saida_pista'][i]}', '{df['ocorrencia_latitude'][i]}', '{df['ocorrencia_longitude'][i]}' )")    



        
        
        print("Deu tudo certo..")
    except Exception as e:
        print(str(e))
