import pandas as pd

all_emiten = pd.read_csv('DaftarSaham.csv')
clean_emiten = all_emiten[all_emiten["Papan Pencatatan"] != "Pemantauan Khusus"].copy()
clean_emiten['Kode_JK'] = clean_emiten['Kode'] + '.JK'
clean_emiten.to_csv('emiten_code_only.csv', columns=['Kode', 'Kode_JK'])