import pandas as pd
import requests
import pprint
import os


def Coleta_Dados(ano, nome_coluna, link_query):
    print('Coletando dados para o ano de', ano)

    
    df_raw = pd.read_csv("Tabela-base.csv")

    # requisicao dos dados
    print("Realizando requisicao")
    requisicao = requests.get(link_query)
    dados_json = requisicao.json()
    print("Requisicao feita")

    # a quantidede de municipios varia de acordo com o indicador
    quant_mun = len(dados_json[0]['resultados'][0]['series'])
    print("Total de municipios: ", quant_mun)

    coleta_cod_mun = []
    coleta_dados = []

    # percorre todos os dados buscando a serie (dado desejado) e localidade (municipio)
    for i in range(quant_mun):
        coleta_cod_mun.append(int(dados_json[0]['resultados'][0]['series'][i]['localidade']['id']))
        #coleta_dados.append((dados_json[0]['resultados'][0]['series'][i]['serie'][ano]/dados_json[0]['resultados'][1]['series'][i]['serie'][ano]))
        coleta_dados.append(dados_json[0]['resultados'][0]['series'][i]['serie'][ano])

    # cria um DataFrame auxilar contendo o codigo do municipio e o dado de interesse coletado
    df_aux = pd.DataFrame({'Cod-MUN':coleta_cod_mun, nome_coluna:coleta_dados})
    df_aux = df_aux.sort_values('Cod-MUN')

    # faz um merge com o DataFrame com os resultados
    df_merge = df_raw.merge(df_aux, left_on='Cod-MUN', right_on='Cod-MUN', how='left')
    df_merge.insert(3, 'Ano', ano)

    # os casos onde certos municipios nao tem o dado de interesse sao preenchidos com a string "n/a"
    #df_merge['Ano'].fillna(ano, inplace=True)
    df_merge[nome_coluna].fillna('-', inplace=True)

    try:
        df_res = pd.read_csv("Tabela-res.csv")
        # concatena o DataFrame merge com o inicial
        df_res = pd.concat([df_res,df_merge])
        df_res.fillna('-')
        df_res = df_res.sort_values(['Cod-MUN', 'Ano'], ascending=[True, True])
        
    except:
        df_res = df_merge

    # salva a tabela resultante em um arquivo csv
    df_res.to_csv("Tabela-res.csv", index=False)

    print("Dados extraidos com sucesso!\n")

def Coleta_Dados_Manual(ano, nome_coluna, arquivo):
    print('Coletando dados para o ano de', ano)

    
    df_raw = pd.read_csv("Tabela-base.csv")

    df_dados = pd.read_excel(arquivo)
    
    coleta_cod_mun = df_dados['muni']
    coleta_dados = df_dados['dados']

    

    # cria um DataFrame auxilar contendo o codigo do municipio e o dado de interesse coletado
    df_aux = pd.DataFrame({'Cod-MUN':coleta_cod_mun, nome_coluna:coleta_dados})
    df_aux = df_aux.sort_values('Cod-MUN')
    
    # faz um merge com o DataFrame com os resultados
    df_merge = df_raw.merge(df_aux, left_on='Cod-MUN', right_on='Cod-MUN', how='left')
    df_merge.insert(3, 'Ano', ano)

    # os casos onde certos municipios nao tem o dado de interesse sao preenchidos com a string "n/a"
    #df_merge['Ano'].fillna(ano, inplace=True)
    df_merge[nome_coluna].fillna('-', inplace=True)

    try:
        df_res = pd.read_csv("Tabela-res.csv")
        # concatena o DataFrame merge com o inicial
        df_res = pd.concat([df_res,df_merge])
        df_res.fillna('-')
        df_res = df_res.sort_values(['Cod-MUN', 'Ano'], ascending=[True, True])
        
    except:
        df_res = df_merge

    print((df_res['dados']).dtypes)
    exit()
    # salva a tabela resultante em um arquivo csv
    df_res.to_csv("Tabela-res.csv", index=False)

    print("Dados extraidos com sucesso!\n")

def Preenche_Tabela_Com_Vazio(ano, nome_coluna):
    # verifica se a tabela base que contem os dados dos municipios ja existe
    print('O ano de', ano, 'nao corresponde ao periodo de coleta.')
    try:
        df_raw = pd.read_csv("Tabela-base.csv")

    except:
        print("Tabela ainda nao existe")
    
        # importa os dados da biblioteca do IBGE
        from ibge.localidades import Municipios
     
        dados_municipios = Municipios()
        nome_municipios = dados_municipios.getNome()
        codigo_municipios = dados_municipios.getId()
        uf_municipios = dados_municipios.getSiglaUF()

        # gera um DataFrame com os dados dos municipios e salva no arquivo csv
        df_raw = pd.DataFrame({'Nome_Mun': nome_municipios[:5], 'SigaUF': uf_municipios[:5], 'Cod-MUN': codigo_municipios[:5]})
        df_raw = df_raw.sort_values('Cod-MUN')
        df_raw.to_csv("Tabela-base.csv", index=False)

    try:
        df_res = pd.read_csv("Tabela-res.csv")
        # concatena o DataFrame merge com o inicial

        df_aux = pd.DataFrame({'Cod-MUN':df_raw['Cod-MUN'], nome_coluna:'-'})
        df_aux = df_aux.sort_values('Cod-MUN')

        #print(df_res.shape)

        df_merge = df_raw.merge(df_aux, left_on='Cod-MUN', right_on='Cod-MUN', how='left')
        df_merge.insert(3, 'Ano', ano)

        #print(df_merge.shape)

        df_res = pd.concat([df_res,df_merge])
        df_res.fillna('-')
        df_res = df_res.sort_values(['Cod-MUN', 'Ano'], ascending=[True, True])
        
    except:
        df_res = df_raw
        df_res['Ano'] = ano
        df_res[nome_coluna] = '-'

    # salva a tabela resultante em um arquivo csv
    df_res.to_csv("Tabela-res.csv", index=False)

    print('Dados vazios gerados.\n')


def Concatena_Tabelas(nome_coluna):
    try:
        df_final = pd.read_csv("Tabela-final.csv")
    except:
        df_final = pd.read_csv("Tabela-res.csv")
        df_final.to_csv("Tabela-final.csv", index=False)
        return
   
    df_res = pd.read_csv("Tabela-res.csv")
    df_final[nome_coluna] = df_res[nome_coluna]

    df_final.to_csv("Tabela-final.csv", index=False)

def Teste_API():
    link_query = 'https://servicodados.ibge.gov.br/api/v3/agregados/6773/periodos/2017/variaveis/183?localidades=N6[1100015,1100023]&classificacao=12598[113455]'
    ano = '2017'
    print("Realizando requisicao")
    requisicao = requests.get(link_query)
    dados_json = requisicao.json()
    print("Requisicao feita")
    pprint.pprint(dados_json)
    exit()

    pprint.pprint(dados_json)
    quant_mun = len(dados_json[0]['resultados'][0]['series'])
    for i in range(quant_mun):

        print(dados_json[0]['resultados'][0]['series'][i]['serie'][ano])
        print(dados_json[0]['resultados'][1]['series'][i]['serie'][ano])


def Equals():
    df1 = pd.read_csv("check_pandas.csv")
    print(df1['Cod-MUN'].equals(df1['Cod-MUN-meu']))
    exit()

def Tabela_Limpa():
    d = pd.read_csv("Tabela-final.csv")
    d = d.replace('-','')
    d.to_csv("Tabela-final-limpa.csv", index=False)
    print(d.head())
    exit()


if __name__ == '__main__':
    
    #Tabela_Limpa()
    #Teste_API()
    
    range_total = range(2009,2023)
    
    # dados referentes a consulta
    range_coleta = range(2017,2018)
    nome_coluna = 'BRUTO_ANACop'
    arquivo = 'tab-anacop.xlsx'

    try:
        os.remove(r'C:\Users\vinicius\source\repos\Query-Builder-SIDRA\Query-Builder-SIDRA\Tabela-res.csv')
    except:
        pass
    
    for ano in range_total:
        if ano in range_coleta:
            link_query = "https://servicodados.ibge.gov.br/api/v3/agregados/6846/periodos/2017/variaveis/183?localidades=N6[all]&classificacao=829[46302]|12598[113455]"
            #Coleta_Dados(str(ano), nome_coluna, link_query)
            Coleta_Dados_Manual(str(ano), nome_coluna, arquivo)
        else:
            Preenche_Tabela_Com_Vazio(str(ano), nome_coluna)

    Concatena_Tabelas(nome_coluna)    









