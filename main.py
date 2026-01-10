import yfinance as yf
import pandas as pd 

df = pd.read_csv('emiten_code_only.csv')
list_of_codes = df['Kode_JK'].to_list()
spaced_codes = ' '.join(list_of_codes)

print(spaced_codes)

# code = yf.Ticker(list_of_codes[1])
# print(f"{code.info['shortName']} {code.info['symbol']} {code.info['currentPrice']}")

# 