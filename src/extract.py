import pandas as pd
import gdown
from pathlib import Path

def download_data(output_file='data.csv'):
    """
    Загружает данные с Google Drive
    """
    try:
        # Загрузка данных
        file_id = '1E1_UYM0Xz8ZHFxvdn15ltgVuuoFZ0b3-'
        url = f'https://drive.google.com/uc?id={file_id}'
        
        print(f"Загрузка данных из Google Drive...")
        gdown.download(url, output_file, quiet=False)
        
        # Проверяем, что файл создан
        if Path(output_file).exists():
            raw_data = pd.read_csv(output_file)
            print(f"✅ Данные успешно загружены. Shape: {raw_data.shape}")
            
            print("Первые 5 строк исходных данных:")
            print(raw_data.head())
            print("\nИнформация о типах данных:")
            print(raw_data.dtypes)
            
            return raw_data
        else:
            print("❌ Не удалось скачать файл")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка скачивания: {e}")
        return None

# Для обратной совместимости
def load_data():
    return download_data()

if __name__ == "__main__":
    download_data()