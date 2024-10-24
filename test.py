import requests
import pandas as pd
import mysql.connector

# Configurações da API
base_url = "http://wapalternativa01.dalla.srv.br:3119/root"
token = "R3385Gti8C4qh45NqYbFe6kTTQ0hsNP03fx21w0u2"  # Substitua pelo seu token
headers = {
    "X-Token": token,
    "Content-Type": "application/json"
}

mysql_user="lamobuser"
mysql_password="AVNS_w50l8PbyCR09wodhFV3"
mysql_host="db-lamob-do-user-17950265-0.m.db.ondigitalocean.com"
mysql_database="dw_lamob"
mysql_port=25060

# Função para obter vendas da fila
def get_vendas():
    response = requests.get(f"{base_url}/venda", headers=headers)
    response.raise_for_status()
    return response.json()["venda"]  # Retorna a lista de vendas

# Função para converter valores de dicionários para strings e tratar NaN
def converter_valores_para_str(row):
    return [str(item) if isinstance(item, (dict, list)) else (None if pd.isna(item) else item) for item in row]


# Função para remover a venda da fila
def remover_venda(codigo_venda):
    response = requests.post(f"{base_url}/venda/{codigo_venda}/ok", headers=headers)
    return response.status_code

# Função principal para processar todas as vendas até que a fila esteja vazia
def processar_todas_as_vendas():
    todas_as_vendas = []  # Para armazenar todas as vendas processadas
    while True:
        vendas = get_vendas()
        
        if not vendas:
            print(f"Nenhuma venda restante na fila. {len(todas_as_vendas)} vendas processadas.")
            break
        
        for venda in vendas:
            codigo_venda = venda.get("Codigo")
            if codigo_venda:
                todas_as_vendas.append(venda)  # Adiciona a venda para salvar no banco
                print(f"Venda {codigo_venda}")
    
    return todas_as_vendas

# Função para converter valores de dicionários para strings
def converter_valores_para_str(row):
    return [str(item) if isinstance(item, (dict, list)) else item for item in row]

# Função para salvar as vendas processadas no banco de dados PostgreSQL
def salvar_vendas_bd(df_novas_vendas):
    # Conectar ao banco de dados PostgreSQL
    conn = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        database=mysql_database,
        user=mysql_user,
        password=mysql_password
    )
    
    cursor = conn.cursor()
    
    # Insere os dados no banco de dados
    for index, row in df_novas_vendas.iterrows():
        row = converter_valores_para_str(row)
        print(row)
        
        column_names = ', '.join([col.lower() for col in df_novas_vendas.columns])
        placeholders = ', '.join(['%s'] * len(df_novas_vendas.columns))

        insert_query = f"INSERT INTO fact_vendas ({column_names}) VALUES ({placeholders})"
        cursor.execute(insert_query, list(row))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Vendas processadas e salvas no banco de dados PostgreSQL")

def vendas_df():
    # Executa o processamento de todas as vendas
    vendas_processadas = processar_todas_as_vendas()

    # Convertendo os dados processados em um DataFrame
    df_novas_vendas = pd.DataFrame(vendas_processadas) 

    return df_novas_vendas

def execucao_full():
    # Salvando as vendas no banco de dados PostgreSQL
    salvar_vendas_bd(vendas_df())

    
print(execucao_full())