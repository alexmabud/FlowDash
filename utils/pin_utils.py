# utils/pin_utils.py
# -*- coding: utf-8 -*-
"""
PIN Utils — validação e helpers de persistência do PIN (4 dígitos) para a tabela `usuarios`.

Resumo
------
- validar_pin(pin_raw) -> str|None: garante exatamente 4 dígitos ou None (vazio).
- set_pin_usuario(conn, usuario_id, pin): atualiza o PIN de um usuário existente.
- create_usuario_with_pin_if_needed(conn, ...): cria usuário com PIN se email não existir; se existir, atualiza PIN.

Notas
-----
- O PIN **não é criptografado** (requisito do projeto); serve só para identificar o vendedor no PDV.
- As triggers criadas na migração já impedem gravar PIN inválido; aqui validamos antes, na aplicação.
"""

from __future__ import annotations
import re
import sqlite3
from typing import Optional, Any, Dict

_PIN_RE = re.compile(r"^\d{4}$")

def validar_pin(pin_raw: Optional[str]) -> Optional[str]:
    """
    Normaliza e valida o PIN.
    - "" ou None -> retorna None.
    - Caso haja conteúdo, exige exatamente 4 dígitos [0-9].
    - Lança ValueError se inválido.
    """
    if pin_raw is None:
        return None
    pin = str(pin_raw).strip()
    if not pin:
        return None
    if not _PIN_RE.match(pin):
        raise ValueError("PIN inválido: use exatamente 4 dígitos (0-9) ou deixe em branco.")
    return pin

def _usuarios_has_column_pin(conn: sqlite3.Connection) -> bool:
    cur = conn.execute("PRAGMA table_info(usuarios)")
    return any(r[1] == "pin" for r in cur.fetchall())

def set_pin_usuario(conn: sqlite3.Connection, usuario_id: int, pin: Optional[str]) -> None:
    """
    Atualiza o PIN de um usuário existente.
    Pré-condição: migração com coluna `pin` aplicada.
    """
    if not _usuarios_has_column_pin(conn):
        raise RuntimeError("A coluna `pin` não existe na tabela `usuarios`. Rode a migração antes.")
    conn.execute("UPDATE usuarios SET pin = ? WHERE id = ?", (pin, usuario_id))
    conn.commit()

def create_usuario_with_pin_if_needed(
    conn: sqlite3.Connection,
    *,
    nome: str,
    email: str,
    senha_hash: str,
    perfil: str = "Vendedor",
    ativo: int = 1,
    pin: Optional[str] = None,
    campos_extras: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Cria um usuário (com PIN) se o email ainda não existir; caso exista, apenas atualiza o PIN.
    Retorna o id do usuário.
    """
    if not _usuarios_has_column_pin(conn):
        raise RuntimeError("A coluna `pin` não existe na tabela `usuarios`. Rode a migração antes.")

    campos_extras = dict(campos_extras or {})
    row = conn.execute("SELECT id FROM usuarios WHERE email = ?", (email,)).fetchone()
    if row:
        user_id = int(row[0])
        conn.execute("UPDATE usuarios SET pin = ? WHERE id = ?", (pin, user_id))
        conn.commit()
        return user_id

    colunas = ["nome", "email", "senha", "perfil", "ativo", "pin"]
    valores = [nome, email, senha_hash, perfil, ativo, pin]

    for k, v in campos_extras.items():
        colunas.append(k)
        valores.append(v)

    placeholders = ", ".join("?" for _ in colunas)
    cols_sql = ", ".join(colunas)
    sql = f"INSERT INTO usuarios ({cols_sql}) VALUES ({placeholders})"

    cur = conn.execute(sql, tuple(valores))
    conn.commit()
    return int(cur.lastrowid)
