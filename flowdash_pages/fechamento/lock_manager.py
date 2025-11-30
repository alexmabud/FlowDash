import sqlite3
from datetime import date

def verificar_pendencia_bloqueante(caminho_banco: str) -> str | None:
    """
    Verifica o banco de dados procurando pelo último dia ANTERIOR a hoje
    que teve movimentação financeira real, mas não tem registro em 'fechamento_caixa'.
    
    Retorna a data (str 'YYYY-MM-DD') se houver pendência, ou None se estiver livre.
    """
    hoje = date.today()
    
    # Query unificada: Vendas + Saídas + Correções + Movimentações Bancárias (Caixa 2/Depósitos)
    query = """
        SELECT MAX(dia_mov) FROM (
            SELECT DATE(data) as dia_mov FROM entrada
            UNION ALL
            SELECT DATE(data) as dia_mov FROM saida
            UNION ALL
            SELECT DATE(data) as dia_mov FROM correcao_caixa
            UNION ALL
            SELECT DATE(data) as dia_mov FROM movimentacoes_bancarias
        ) 
        WHERE dia_mov < DATE(?)
    """
    
    try:
        with sqlite3.connect(caminho_banco) as conn:
            cursor = conn.cursor()
            
            # 1. Busca a última data movimentada antes de hoje
            cursor.execute(query, (hoje,))
            row = cursor.fetchone()
            
            # Se nunca houve movimento ou banco é novo
            if not row or not row[0]:
                return None 
                
            ultima_data_ativa = row[0]
            
            # 2. Verifica se essa data específica já consta na tabela de fechamento
            cursor.execute(
                "SELECT 1 FROM fechamento_caixa WHERE DATE(data) = DATE(?) LIMIT 1", 
                (ultima_data_ativa,)
            )
            is_fechado = cursor.fetchone()
            
            # Se NÃO achou o fechamento, retorna a data para bloquear o sistema
            if not is_fechado:
                return ultima_data_ativa

    except Exception:
        # Se tabelas não existirem (banco vazio), não bloqueia
        return None


    return None


def verificar_se_dia_esta_fechado(caminho_banco: str, data_alvo: date) -> bool:
    """
    Retorna True se o dia alvo já possui um registro na tabela fechamento_caixa.
    Isso impede edições em dias já encerrados.
    """
    try:
        with sqlite3.connect(caminho_banco) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM fechamento_caixa WHERE DATE(data) = DATE(?) LIMIT 1", 
                (data_alvo,)
            )
            return bool(cursor.fetchone())
    except Exception:
        return False
