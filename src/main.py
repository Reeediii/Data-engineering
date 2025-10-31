import argparse
import sys
import os
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description='ETL Pipeline for Data Processing')
    parser.add_argument('--download', action='store_true', help='Download data from Google Drive')
    parser.add_argument('--transform', action='store_true', help='Transform data types')
    parser.add_argument('--load', action='store_true', help='Load data to database')
    parser.add_argument('--all', action='store_true', help='Run complete ETL pipeline')
    parser.add_argument('--input-file', type=str, default='data.csv', help='Input CSV file path')
    parser.add_argument('--processed-file', type=str, default='data_processed.parquet', help='Processed data file path')
    parser.add_argument('--table-name', type=str, default='bazhanov', help='Database table name')
    
    args = parser.parse_args()
    
    # Если не указаны аргументы, показываем помощь
    if not any([args.download, args.transform, args.load, args.all]):
        parser.print_help()
        return
    
    try:
        if args.all or args.download:
            print("=== ШАГ 1: ЗАГРУЗКА ДАННЫХ ===")
            from extract import download_data
            download_data()
        
        if args.all or args.transform:
            print("\n=== ШАГ 2: ПРИВЕДЕНИЕ ТИПОВ ===")
            from transform import transform_data_types
            df_transform = transform_data_types(args.input_file, args.processed_file)
            if df_transform is None:
                print("❌ Приведение типов провалено!")
                return
        
        if args.all or args.load:
            print("\n=== ШАГ 3: ЗАГРУЗКА В БАЗУ ДАННЫХ ===")
            from load import load_to_database
            load_to_database(args.processed_file, args.table_name)
        
        print("\n✅ ETL успешно завершен!")
        
    except Exception as e:
        print(f"❌ ETL провалился: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()