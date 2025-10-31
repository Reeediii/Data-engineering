import pandas as pd
from pathlib import Path

def transform_data_types(input_file='data.csv', output_file='data_processed.parquet'):
    """
    Преобразует типы данных в датасете
    """
    try:
        # Проверяем существование входного файла
        if not Path(input_file).exists():
            print(f"❌ Input file {input_file} not found!")
            return None
            
        # Чтение данных
        print(f"Reading data from {input_file}...")
        raw_data = pd.read_csv(input_file)
        df = raw_data.copy()
        
        print(f"📊 Original data shape: {df.shape}")
        
        # Приведение типов данных
        # Категориальные данные
        categorical_columns = ['Description', 'Name', 'iso_code']
        for col in categorical_columns:
            if col in df.columns:
                df[col] = df[col].astype('category')
                print(f"✅ Converted to categorical: {col}")
        
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
                original_null_count = df[col].isnull().sum()
                df[col] = pd.to_numeric(df[col], errors='coerce')
                new_null_count = df[col].isnull().sum()
                if new_null_count > original_null_count:
                    print(f"🔢 Converted to numeric: {col} (+{new_null_count - original_null_count} nulls)")
        
        # Год как целое число
        if 'year' in df.columns:
            df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(0).astype('int64')
            print("✅ Converted: year -> int64")
        
        # Население как целое число
        if 'population' in df.columns:
            df['population'] = pd.to_numeric(df['population'], errors='coerce').fillna(0).astype('int64')
            print("✅ Converted: population -> int64")
        
        print("\n" + "="*50)
        print("📋 Data types after transformation:")
        print(df.dtypes)
        
        # Сохранение данных
        try:
            if output_file.endswith('.parquet'):
                df.to_parquet(output_file, index=False)
            else:
                df.to_csv(output_file, index=False)
            print(f"💾 Data saved to: {output_file}")
        except Exception as e:
            print(f"⚠ Could not save as Parquet: {e}")
            output_file = 'data_processed.csv'
            df.to_csv(output_file, index=False)
            print(f"💾 Data saved to: {output_file}")
        
        print(f"📈 Final dataset shape: {df.shape}")
        
        # Анализ пропущенных значений
        missing_data = df.isnull().sum()
        if missing_data.sum() > 0:
            print(f"🔍 Missing values by column:")
            print(missing_data[missing_data > 0].sort_values(ascending=False).head(10))
        else:
            print("🔍 No missing values found")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка приведения типов: {e}")
        return None

# Для обратной совместимости
if __name__ == "__main__":
    transform_data_types()