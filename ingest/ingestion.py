import yfinance as yf
import pandas as pd 
import time 

BATCH_SIZE = 50

df = pd.read_csv('emiten_code_only.csv')
list_of_codes = df['Kode_JK'].to_list()

for i in range(0, len(list_of_codes), BATCH_SIZE):
    yf_data = yf.download(list_of_codes[i:i+BATCH_SIZE])
    time.sleep(2)
