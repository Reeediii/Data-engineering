import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

def load_to_database(input_file='data_processed.parquet', table_name='bazhanov'):
    """
    Загружает данные в базу данных
    """
    try:
        # Проверяем существование файла
        if not Path(input_file).exists():
            print(f"❌ Processed file {input_file} not found!")
            return False
            
        # Чтение учетных данных
        print("🔑 Reading database credentials...")
        conn = sqlite3.connect("creds.db")
        cur = conn.cursor()

        # Получаем учетные данные
        cur.execute('SELECT url, port, user, pass FROM access LIMIT 1;')
        row = cur.fetchone()
        conn.close()

        if not row:
            print("❌ No credentials found in creds.db")
            return False
            
        url, port, user, password = row
        dbname = 'homeworks' 

        print('✅ Credentials loaded successfully:')
        print(f'   URL: {url}:{port}')
        print(f'   Database: {dbname}')
        print(f'   User: {user}')

        # Подключение к PostgreSQL
        print('\n🔗 Connecting to PostgreSQL...')
        conn_str = f'postgresql+psycopg2://{user}:{password}@{url}:{port}/{dbname}'
        engine = create_engine(conn_str)
        
        # Тестируем подключение
        with engine.connect() as test_conn:
            test_conn.execute(text("SELECT 1"))
        print('✅ PostgreSQL connection established')

        # Загрузка датасета
        print(f'\n📂 Loading dataset from {input_file}')
        if input_file.endswith('.parquet'):
            df = pd.read_parquet(input_file)
        else:
            df = pd.read_csv(input_file)
            
        print(f'✅ Dataset loaded. Rows: {len(df)}, Columns: {len(df.columns)}')

        # Ограничиваем количество строк для демонстрации
        df_load = df.head(100)
        print(f'📊 Prepared {len(df_load)} rows for database insertion')

        # Запись данных в БД
        print(f'\n💾 Writing data to table "{table_name}"')
        df_load.to_sql(table_name, engine, schema="public", if_exists="replace", index=False)
        print(f'✅ Table "{table_name}" successfully created ({len(df_load)} rows)')

        # Проверка записи
        print('\n🔍 Verifying data in database...')
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {table_name};'))
            count = result.scalar()
            print(f'✅ Table contains {count} rows')
            
            result = conn.execute(text(f'SELECT * FROM {table_name} LIMIT 3;'))
            rows = result.fetchall()

        if rows:
            print(f'📋 Sample data from "{table_name}":')
            for r in rows:
                print(r)
        
        print('✅ Data loading completed successfully!')
        return True
        
    except Exception as e:
        print(f'❌ Database loading error: {e}')
        return False

if __name__ == "__main__":
    load_to_database()