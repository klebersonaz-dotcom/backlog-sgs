pandas==2.2.2
SQLAlchemy==2.0.30
pyodbc==5.1.0
requests==2.32.3
# -*- coding: utf-8 -*-
import os
import json
import logging
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sqlalchemy import create_engine, text
import urllib

# ————————————————————————————————————————————————
# LOG
# ————————————————————————————————————————————————
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("backlog_job.log", encoding="utf-8")]
)

# ————————————————————————————————————————————————
# CONFIGURAÇÕES DE CONEXÃO COM SQL SERVER
# (dica: use variáveis de ambiente no lugar dos literais)
# ————————————————————————————————————————————————
server = os.getenv('SQL_SERVER', '198.96.95.218')
database = os.getenv('SQL_DB', 'BI_SGS')
username = os.getenv('SQL_USER', 'kleberson.mariano')
password = os.getenv('SQL_PWD', '148491Dani@')

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", future=True)

# ————————————————————————————————————————————————
# API
# ————————————————————————————————————————————————
base_url = os.getenv('API_BASE_URL', "http://api.vivo.atlanteti.com")
endpoint = "/v1/relatorio/estoque-reparador/list-serial"
headers = {"Authorization": os.getenv('API_TOKEN', "9CwxE1UedgAvEd9fMUp")}  # mantenho como no seu script

# ————————————————————————————————————————————————
# BACKFILL MANUAL (assumi 2025-08-XX; ajuste se necessário)
# ————————————————————————————————————————————————
MANUAL_BACKFILL = {
    "2025-08-01": 545,
    "2025-08-04": 591,
    "2025-08-05": 557,
    "2025-08-06": 549,
    "2025-08-07": 521,
    "2025-08-08": 499,
    "2025-08-11": 508,
    "2025-08-12": 514,
    "2025-08-13": 493,
    "2025-08-14": 475,
    "2025-08-18": 591,
    "2025-08-19": 613,
    "2025-08-20": 609,
}

# ————————————————————————————————————————————————
# HTTP session com retry/backoff
# ————————————————————————————————————————————————
def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",)
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# ————————————————————————————————————————————————
# Snapshot local (meia-noite America/Sao_Paulo, sem tz no SQL)
# ————————————————————————————————————————————————
def today_midnight_local_naive() -> datetime:
    tz = ZoneInfo("America/Sao_Paulo")
    today_local = datetime.now(tz).date()
    # grava como "naive" porém já no horário local (00:00:00)
    return datetime.combine(today_local, time(0, 0, 0))

# ————————————————————————————————————————————————
# Coleta e filtro (AMERINODE & dias>30)
# ————————————————————————————————————————————————
def coletar_backlog(session: requests.Session) -> list:
    filtrados = []
    page = 1
    while True:
        url = f"{base_url}{endpoint}?page={page}"
        resp = session.get(url, headers=headers, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"Erro na página {page}: {resp.status_code} - {resp.text[:300]}")
        try:
            j = resp.json()
        except json.JSONDecodeError:
            raise RuntimeError(f"Resposta não-JSON na página {page}: {resp.text[:300]}")
        page_data = j.get("data", [])
        if not page_data:
            break

        for item in page_data:
            nome_forn = str(item.get("nomeFornecedor", "")).upper()
            dias = item.get("dias", None)
            if dias is None:
                continue
            if ("AMERINODE" in nome_forn) and (dias > 30):
                filtrados.append(item)

        page += 1

    return filtrados

# ————————————————————————————————————————————————
# Upsert do snapshot diário
# ————————————————————————————————————————————————
def gravar_snapshot_diario(conn, data_snapshot: datetime, qtd_vencido: int, refresh_today=True):
    if refresh_today:
        # upsert: atualiza se já existir, senão insere
        conn.execute(text("""
IF EXISTS (
    SELECT 1
      FROM tbl_backlog_historico
     WHERE CAST(Data_Snapshot AS DATE) = CAST(:ds AS DATE)
)
    UPDATE tbl_backlog_historico
       SET Qtd_Backlog_Vencido = :qtd
     WHERE CAST(Data_Snapshot AS DATE) = CAST(:ds AS DATE);
ELSE
    INSERT INTO tbl_backlog_historico (Data_Snapshot, Qtd_Backlog_Vencido)
    VALUES (:ds, :qtd);
        """), {"ds": data_snapshot, "qtd": int(qtd_vencido)})
    else:
        # comportamento antigo: só insere se não existir
        conn.execute(text("""
IF NOT EXISTS (
    SELECT 1 FROM tbl_backlog_historico
    WHERE CAST(Data_Snapshot AS DATE) = CAST(:ds AS DATE)
)
    INSERT INTO tbl_backlog_historico (Data_Snapshot, Qtd_Backlog_Vencido)
    VALUES (:ds, :qtd);
        """), {"ds": data_snapshot, "qtd": int(qtd_vencido)})


# ————————————————————————————————————————————————
# Preenche datas faltantes (NULL)
# ————————————————————————————————————————————————
def preencher_lacunas(conn, df_hist: pd.DataFrame, ate_data: datetime):
    if df_hist.empty:
        return
    df_hist = df_hist.copy()
    df_hist["Data_Snapshot"] = pd.to_datetime(df_hist["Data_Snapshot"]).dt.normalize()

    datas_esperadas = pd.date_range(df_hist["Data_Snapshot"].min().date(), ate_data.date(), freq="D")
    datas_existentes = set(df_hist["Data_Snapshot"].dt.date)
    faltando = sorted(set(datas_esperadas.date) - datas_existentes)

    for d in faltando:
        conn.execute(text("""
            INSERT INTO tbl_backlog_historico (Data_Snapshot, Qtd_Backlog_Vencido)
            VALUES (:ds, NULL)
        """), {"ds": datetime.combine(d, time(0,0,0))})

# ————————————————————————————————————————————————
# Backfill manual (upsert)
# ————————————————————————————————————————————————
def backfill_manual(conn, dicionario: dict):
    for k, v in dicionario.items():
        ds = datetime.strptime(k, "%Y-%m-%d")
        ja_existe = conn.execute(text("""
            SELECT 1
            FROM tbl_backlog_historico
            WHERE CAST(Data_Snapshot AS DATE) = CAST(:ds AS DATE)
        """), {"ds": ds}).fetchone()

        if ja_existe:
            conn.execute(text("""
                UPDATE tbl_backlog_historico
                SET Qtd_Backlog_Vencido = :qtd
                WHERE CAST(Data_Snapshot AS DATE) = CAST(:ds AS DATE)
            """), {"ds": ds, "qtd": int(v)})
        else:
            conn.execute(text("""
                INSERT INTO tbl_backlog_historico (Data_Snapshot, Qtd_Backlog_Vencido)
                VALUES (:ds, :qtd)
            """), {"ds": ds, "qtd": int(v)})

# ————————————————————————————————————————————————
# Agregação semanal (último snapshot da semana)
# ————————————————————————————————————————————————
def atualizar_agregado_semanal(conn):
    df = pd.read_sql(
        "SELECT Data_Snapshot, Qtd_Backlog_Vencido FROM tbl_backlog_historico",
        conn.connection,
    )
    if df.empty:
        return

    df["Data_Snapshot"] = pd.to_datetime(df["Data_Snapshot"]).dt.normalize()
    df["Inicio_Semana"] = df["Data_Snapshot"] - pd.to_timedelta(df["Data_Snapshot"].dt.weekday, unit="D")
    df["Fim_Semana"] = df["Inicio_Semana"] + pd.Timedelta(days=6)
    df["Week"] = df["Inicio_Semana"].dt.isocalendar().week.astype(int)

    def pick_last_non_null(g):
        g = g.sort_values("Data_Snapshot")
        g2 = g[g["Qtd_Backlog_Vencido"].notna()]
        # pega o último NÃO-NULL; se todos forem NULL, mantém NULL do último dia
        row = g2.iloc[-1] if not g2.empty else g.iloc[-1]
        return pd.Series({
            "Inicio_Semana": g["Inicio_Semana"].iloc[0],
            "Fim_Semana": g["Fim_Semana"].iloc[0],
            "Week": g["Week"].iloc[0],
            "Qtd_Backlog_Vencido": row["Qtd_Backlog_Vencido"],
        })

    weekly = df.groupby("Inicio_Semana", as_index=False).apply(pick_last_non_null).reset_index(drop=True)
    weekly["Intervalo_Semana"] = (
        weekly["Inicio_Semana"].dt.strftime("%d/%m") + " a " + weekly["Fim_Semana"].dt.strftime("%d/%m")
    )

    for _, r in weekly.iterrows():
        conn.execute(text("""
            MERGE dbo.tbl_backlog_historico_semanal AS T
            USING (SELECT :ini AS Inicio_Semana) AS S
                 ON T.Inicio_Semana = S.Inicio_Semana
            WHEN MATCHED THEN
                UPDATE SET Fim_Semana = :fim,
                           Week = :week,
                           Qtd_Backlog_Vencido = :qtd,
                           Intervalo_Semana = :intv
            WHEN NOT MATCHED THEN
                INSERT (Inicio_Semana, Fim_Semana, Week, Qtd_Backlog_Vencido, Intervalo_Semana)
                VALUES (:ini, :fim, :week, :qtd, :intv);
        """), {
            "ini": r["Inicio_Semana"], "fim": r["Fim_Semana"], "week": int(r["Week"]),
            "qtd": int(r["Qtd_Backlog_Vencido"]) if pd.notnull(r["Qtd_Backlog_Vencido"]) else None,
            "intv": r["Intervalo_Semana"]
        })

# ————————————————————————————————————————————————
# MAIN
# ————————————————————————————————————————————————
def main():
    session = build_session()
    logging.info("Coletando páginas da API...")
    filtrados = coletar_backlog(session)
    df_backlog = pd.DataFrame(filtrados)
    qtd_vencido = int(len(df_backlog))
    data_snapshot = today_midnight_local_naive()

    logging.info(f"Backlog AMERINODE (>30): {qtd_vencido} | Snapshot: {data_snapshot.date()}")

    with engine.begin() as conn:
        # grava o snapshot do dia (idempotente)
        gravar_snapshot_diario(conn, data_snapshot, qtd_vencido)

        # carrega histórico para lacunas
        df_hist = pd.read_sql("SELECT * FROM tbl_backlog_historico", conn.connection)

        # preenche datas faltantes com NULL
        preencher_lacunas(conn, df_hist, data_snapshot)

        # aplica backfill manual (upsert das datas anotadas)
        backfill_manual(conn, MANUAL_BACKFILL)

        # atualiza o agregado semanal
        atualizar_agregado_semanal(conn)

    logging.info(f"✅ Concluído com sucesso! AMERINODE backlog (>30): {qtd_vencido} registros")

if __name__ == "__main__":
    main()
