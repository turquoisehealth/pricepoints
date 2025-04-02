import pandas as pd
import tq

# Example script using the built-in tq module
trino_conn = tq.get_trino_connection()

query = """
    select *
    from hive.public_latest.core_rates
    where payer_id = '76'
    limit 100
"""

df = pd.read_sql_query(query, trino_conn)
print(df)
