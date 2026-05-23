import sqlite3
import pandas as pd
from config.config_fetch import DB_PATH

def overview_indicators(conn):
    print("\n=== INDICATORS: General Info ===")
    df = pd.read_sql("SELECT * FROM indicators", conn)
    print(f"Total rows: {len(df)}")
    if df.empty:
        print("No data in indicators table.")
        return

    print("\nRows per country:")
    print(df['country'].value_counts())

    print("\nRows per category:")
    print(df['category'].value_counts())

    print("\nRows per indicator_name:")
    print(df['indicator_name'].value_counts())

    print("\nRows per (country, category):")
    print(df.groupby(['country', 'category']).size())

    print("\nDate range per (country, category, indicator_name):")
    grouped = df.groupby(['country', 'category', 'indicator_name'])['date']
    print(grouped.agg(['min', 'max', 'count']))

    print("\nSample rows per (country, category):")
    for (country, category), group in df.groupby(['country', 'category']):
        print(f"\nSample for {country} / {category}:")
        print(group.head(2)[['date', 'indicator_name', 'value', 'unit', 'impact']])

def overview_table(conn, table, n=5):
    print(f"\n=== {table.upper()} ===")
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"Rows: {count}")
        df = pd.read_sql(f"SELECT * FROM {table} LIMIT {n}", conn)
        print(df)
    except Exception as e:
        print(f"Could not read table {table}: {e}")

def main():
    conn = sqlite3.connect(DB_PATH)
    overview_indicators(conn)
    overview_table(conn, "gold_price")
    overview_table(conn, "events")
    conn.close()

if __name__ == "__main__":
    main()