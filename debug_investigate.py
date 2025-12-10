
import sqlite3
import pandas as pd
import json
from datetime import date

DB_PATH = r"c:\Users\User\OneDrive\Documentos\Python\FlowDash\data\flowdash_data.db"

def _norm(s):
    if not isinstance(s, str): return s
    return s.strip().lower()

try:
    conn = sqlite3.connect(DB_PATH)
    
    # Check Taxas for duplicates
    print("\n--- Verifying Taxas Duplicates ---")
    df_taxas = pd.read_sql("SELECT maquineta, forma_pagamento, bandeira, parcelas, banco_destino FROM taxas_maquinas", conn)
    
    df_taxas['k_maq'] = df_taxas['maquineta'].astype(str).str.strip().str.upper()
    df_taxas['k_forma'] = df_taxas['forma_pagamento'].astype(str).str.strip().str.upper()
    df_taxas['k_band'] = df_taxas['bandeira'].astype(str).str.strip().str.upper()
    df_taxas['k_parc'] = pd.to_numeric(df_taxas['parcelas'], errors='coerce').fillna(1).astype(int)
    
    dup_taxas = df_taxas[df_taxas.duplicated(subset=['k_maq', 'k_forma', 'k_band', 'k_parc'], keep=False)]
    if not dup_taxas.empty:
        print("!!! DUPLICATES FOUND IN TAXAS !!!")
        print(dup_taxas.sort_values(by=['k_maq']))
    else:
        print("No duplicates in Taxas.")

    # Simulate Logic for Date 10/12
    print("\n--- Simulating Calculation ---")
    dt_base_str = '2025-12-09'
    data_alvo_str = '2025-12-10'
    
    # Get Sales
    df_vendas = pd.read_sql(f"""
            SELECT maquineta, Forma_de_Pagamento, Bandeira, Parcelas, valor_liquido 
            FROM entrada 
            WHERE DATE(Data_Liq) > DATE('{dt_base_str}') AND DATE(Data_Liq) <= DATE('{data_alvo_str}')
        """, conn)
    
    print(f"Sales Found: {len(df_vendas)} rows")
    print(f"Total Liquido (Pre-Merge): {df_vendas['valor_liquido'].sum()}")

    # Normalize Sales
    df_vendas['k_maq'] = df_vendas['maquineta'].astype(str).str.strip().str.upper()
    df_vendas['k_forma'] = df_vendas['Forma_de_Pagamento'].astype(str).str.strip().str.upper()
    df_vendas['k_band'] = df_vendas['Bandeira'].astype(str).str.strip().str.upper()
    df_vendas['k_parc'] = pd.to_numeric(df_vendas['Parcelas'], errors='coerce').fillna(1).astype(int)
    
    # Merge
    df_merged = pd.merge(df_vendas, df_taxas[['k_maq', 'k_forma', 'k_band', 'k_parc', 'banco_destino']], 
                         on=['k_maq', 'k_forma', 'k_band', 'k_parc'], how='left')
    
    print(f"Rows After Merge: {len(df_merged)}")
    print(f"Total Liquido (Post-Merge): {df_merged['valor_liquido'].sum()}")
    
    if len(df_merged) > len(df_vendas):
        print("!!! ROW EXPLOSION DETECTED !!!")

except Exception as e:
    print(e)
