import pandas as pd
import gdown
file_id = '1E1_UYM0Xz8ZHFxvdn15ltgVuuoFZ0b3-'
url = f'https://drive.google.com/uc?id={file_id}'
output_file = 'data.csv'
gdown.download(url, output_file, quiet=False)
raw_data = pd.read_csv(output_file)
print(raw_data.head(10))