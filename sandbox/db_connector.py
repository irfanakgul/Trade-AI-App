from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
import re


#  connection into cloud db (central)
def cloud_connection():
    url = URL.create(
    "postgresql+psycopg2",
    username="irfan_admin",
    password="TradeAPP_IA_2026@@",
    host="95.216.148.216",
    port=5432,
    database="trade_app",
)

    engine = create_engine(url, pool_pre_ping=True)

    with engine.connect() as conn:
        print('========|',conn.execute(text("SELECT current_user, current_database()")).fetchone(),'|========')

    print('*** ✅ SUCCESSFUL CLOUD CONNECTION ⛓️ ***')
    
    return engine

engine = cloud_connection()

# needs to read correct column names from cloud db
def read_sql_case_safe(engine, query: str):

    q = query.strip().rstrip(";")
    m = re.match(r'(?is)^\s*select\s+(?P<select>.+?)\s+from\s+(?P<table>[A-Za-z0-9_]+)\s*(?P<rest>.*)$', q)
    if not m:
        # tanıyamazsa aynen çalıştır
        return pd.read_sql_query(query, engine)

    select_part = m.group("select").strip()
    table = m.group("table").strip()
    rest = m.group("rest") or ""

    # tablonun gerçek kolon adlarını çek
    cols = pd.read_sql_query(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
        ORDER BY ordinal_position
    """), engine, params={"t": table})["column_name"].tolist()

    # kolonu case-insensitive eşle
    col_map = {c.lower(): c for c in cols}

    if select_part == "*":
        fixed_select = "*"
    else:
        raw_cols = [c.strip() for c in select_part.split(",")]
        fixed_cols = []
        for rc in raw_cols:
            # fonksiyon/alias gibi şeyler varsa dokunmayalım
            if re.search(r'\(|\)|\s+as\s+|\s', rc, flags=re.I):
                fixed_cols.append(rc)
                continue

            real = col_map.get(rc.lower())
            if real:
                fixed_cols.append(f'"{real}"')
            else:
                fixed_cols.append(rc)
        fixed_select = ", ".join(fixed_cols)

    fixed_query = f'SELECT {fixed_select} FROM "{table}" {rest}'.strip()
    return pd.read_sql_query(fixed_query, engine)



#read from cloud
def fn_read_data_cloud(schema, table_name, attributes=None):
    # table name validation
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError("Invalid table name")
    if not re.fullmatch(r"[A-Za-z0-9_]+", schema):
        raise ValueError("Invalid schema name")
        
    # select cols
    if attributes is None:
        cols = '*'
    else:
        # kolon validation + quoting
        safe_cols = []
        for col in attributes:
            if not re.fullmatch(r"[A-Za-z0-9_]+", col):
                raise ValueError(f"Invalid column name: {col}")
            safe_cols.append(f'"{col}"')
        cols = ', '.join(safe_cols)
        
    # schema ve tablo ayrı ayrı tırnaklanmalı
    query = f'SELECT {cols} FROM "{schema}"."{table_name}"'
    
    return read_sql_case_safe(engine, query)

#write to cloud
def fn_write_cloud(df, schema, name, if_exists,is_only_distinct = False,dist_col=None):
    #add/create direct
    if is_only_distinct == False:
        df.to_sql(name=name, con=engine,schema=schema, if_exists=if_exists, index=False)
    
    # check dist col, if dist, then append
    elif is_only_distinct == True:
        lst_exist_rows = list(fn_read_data_cloud(table_name=name)[dist_col])
        if dist_col not in lst_exist_rows:
            df_dist = df[~df[dist_col].isin(lst_exist_rows)]
            df_dist.to_sql(name=name, con=engine,schema=schema, if_exists='append', index=False)
            print(f'distinct data count {len(df_dist)} | COL: {name}')

# read only filtered symbol
def fn_cloud_read_table_where_symbol(schema,table_name,SYMBOL):
    # schema ve tablo ayrı ayrı tırnaklanmalı
    query = f'SELECT * FROM "{schema}"."{table_name}" WHERE SYMBOL = "{SYMBOL}"'
    return read_sql_case_safe(engine, query)




#create schema
def create_schema(schema_name, engine=engine):

    if not re.fullmatch(r"[A-Za-z0-9_]+", schema_name):
        raise ValueError("Invalid schema name")

    schema_name_clean = schema_name.lower()

    with engine.begin() as conn:

        # case-insensitive kontrol
        exists = conn.execute(
            text("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE lower(schema_name) = lower(:schema_name)
            """),
            {"schema_name": schema_name_clean}
        ).scalar()

        if exists:
            raise ValueError(f"Schema '{exists}' already exists")

        # her zaman lower-case oluştur (PostgreSQL best practice)
        conn.execute(
            text(f'CREATE SCHEMA "{schema_name_clean}"'))
        
def run_postgres_sql(sql: str, params: dict | None = None, fetch: bool = True):
    url = URL.create(
        "postgresql+psycopg2",
        username="irfan_admin",
        password="TradeAPP_IA_2026@@",
        host="95.216.148.216",
        port=5432,
        database="trade_app",
    )

    engine = create_engine(url)

    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql), params or {})

            if fetch and result.returns_rows:
                rows = result.mappings().all()
                return [dict(r) for r in rows]

            return {
                "status": "ok",
                "rowcount": result.rowcount,
            }

    except Exception as e:
        raise RuntimeError(f"SQL çalıştırma hatası: {e}")
    finally:
        engine.dispose()