# Importador_Sidra

O código para a extração de dados a partir da API de dados agregados do IBGE está disponível no arquivo 'Query_Builder_SIDRA.py' e utiliza as bibliotecas Pandas, para análise e manipulação de dados, e Request, para leitura das requisições pela API, ambas da linguagem Python.

O link para a realização da requisição foi gerado manualmente pelo Query Builder da API (https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq). A partir daí, o código se encarrega da leitura e obtenção dos dados.

Duas funções principais fazem a coleta e tratamento dos dados: 'Coleta_Dados' e 'Coleta_Dados_Manual'. Ambas têm por lógica realizar a requisição e criar um novo Data Frame com o id (código do município) e série (dado de interesse) disponibilizados pela API através de um JSON. A diferença entre as funções é que a 'Coleta_Dados_Manual' faz a coleta a partir de arquivos CSV baixados localmente enquanto a 'Coleta_Dados' faz diretamente pela query gerada pela API. Isto foi necessário pois, em alguns casos, as requisições da API não tinham resposta.

Nem todos os dados para coleta estão disponibilizados para todo o período de tempo dos anos de 2014 a 2022. Por isto, foi gerado também dados vazios, a fim de preencher a tabela resultante e deixá-la no formato correto. Ao final da leitura dos dados, a coluna de interesse gerada, juntamente com os dados em branco (se aplicável), foi concatenada à tabela final de todos os dados da base SIDRA.
