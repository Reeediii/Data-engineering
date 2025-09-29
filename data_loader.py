import pandas as pd
import gdown

def load_and_process_data():
    # Загрузка данных
    file_id = '1E1_UYM0Xz8ZHFxvdn15ltgVuuoFZ0b3-'
    url = f'https://drive.google.com/uc?id={file_id}'
    output_file = 'data.csv'
    gdown.download(url, output_file, quiet=False)
    raw_data = pd.read_csv(output_file)
    
    print("Первые 10 строк исходных данных:")
    print(raw_data.head(10))
    print("\nИнформация о типах данных до преобразования:")
    print(raw_data.dtypes)
    
    # Создаем копию данных для преобразования
    df = raw_data.copy()
    
    # Приведение типов данных
    # Категориальные данные
    categorical_columns = ['Description', 'Name', 'iso_code']
    for col in categorical_columns:
        if col in df.columns:
            df[col] = df[col].astype('category')
    
    # Числовые данные
    numeric_columns = [
        'year', 'population', 'gdp', 'cement_co2', 'cement_co2_per_capita', 
        'co2', 'co2_growth_abs', 'co2_growth_prct', 'co2_including_luc',
        'co2_including_luc_growth_abs', 'co2_including_luc_growth_prct',
        'co2_including_luc_per_capita', 'co2_including_luc_per_gdp',
        'co2_including_luc_per_unit_energy', 'co2_per_capita', 'co2_per_gdp',
        'co2_per_unit_energy', 'coal_co2', 'coal_co2_per_capita',
        'consumption_co2', 'consumption_co2_per_capita', 'consumption_co2_per_gdp',
        'cumulative_cement_co2', 'cumulative_co2', 'cumulative_co2_including_luc',
        'cumulative_coal_co2', 'cumulative_flaring_co2', 'cumulative_gas_co2',
        'cumulative_luc_co2', 'cumulative_oil_co2', 'cumulative_other_co2',
        'energy_per_capita', 'energy_per_gdp', 'flaring_co2', 'flaring_co2_per_capita',
        'gas_co2', 'gas_co2_per_capita', 'ghg_excluding_lucf_per_capita',
        'ghg_per_capita', 'land_use_change_co2', 'land_use_change_co2_per_capita',
        'methane', 'methane_per_capita', 'nitrous_oxide', 'nitrous_oxide_per_capita',
        'oil_co2', 'oil_co2_per_capita', 'other_co2_per_capita', 'other_industry_co2',
        'primary_energy_consumption', 'share_global_cement_co2', 'share_global_co2',
        'share_global_co2_including_luc', 'share_global_coal_co2',
        'share_global_cumulative_cement_co2', 'share_global_cumulative_co2',
        'share_global_cumulative_co2_including_luc', 'share_global_cumulative_coal_co2',
        'share_global_cumulative_flaring_co2', 'share_global_cumulative_gas_co2',
        'share_global_cumulative_luc_co2', 'share_global_cumulative_oil_co2',
        'share_global_cumulative_other_co2', 'share_global_flaring_co2',
        'share_global_gas_co2', 'share_global_luc_co2', 'share_global_oil_co2',
        'share_global_other_co2', 'share_of_temperature_change_from_ghg',
        'temperature_change_from_ch4', 'temperature_change_from_co2',
        'temperature_change_from_ghg', 'temperature_change_from_n2o',
        'total_ghg', 'total_ghg_excluding_lucf', 'trade_co2', 'trade_co2_share'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            # Заменяем пустые строки на NaN и преобразуем в float
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Год как целое число
    if 'year' in df.columns:
        df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(0).astype('int64')
    
    # Население как целое число
    if 'population' in df.columns:
        df['population'] = pd.to_numeric(df['population'], errors='coerce').fillna(0).astype('int64')
    
    print("\nИнформация о типах данных после преобразования:")
    print(df.dtypes)
    
    print("\nБазовая статистика по числовым колонкам:")
    print(df.describe())
    
    # Сохранение в один формат - Parquet (рекомендуемый для больших данных)
    df.to_parquet('data_processed.parquet', index=False)
    
    print(f"\nДанные сохранены в формате: data_processed.parquet")
    
    print(f"\nРазмер исходного датасета: {df.shape}")
    print(f"Количество пропущенных значений по колонкам:")
    print(df.isnull().sum().sort_values(ascending=False).head(10))
    
    return df

# Если файл запускается напрямую
if __name__ == "__main__":
    processed_data = load_and_process_data()
    print("\nОбработка данных завершена!")