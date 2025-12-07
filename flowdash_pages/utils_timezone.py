import pytz
from datetime import datetime, date

def hoje_br() -> date:
    """Retorna a data de hoje no fuso horário de Brasília (America/Sao_Paulo)."""
    br_tz = pytz.timezone('America/Sao_Paulo')
    return datetime.now(br_tz).date()

def agora_br() -> datetime:
    """Retorna o datetime atual no fuso horário de Brasília."""
    br_tz = pytz.timezone('America/Sao_Paulo')
    return datetime.now(br_tz)
