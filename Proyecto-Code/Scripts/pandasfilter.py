import pandas as pd

df=pd.read_csv("Archivos filtrados Spark/EthereumFilter1.csv")

for i in df.index:
    df.loc[i, 'Ultimo']=float(str(df.loc[i, 'Ultimo']).replace('.','').replace(',', '.'))
    df.loc[i, 'Apertura']=float(str(df.loc[i, 'Apertura']).replace('.','').replace(',', '.'))
    df.loc[i, 'Maximo']=float(str(df.loc[i, 'Maximo']).replace('.','').replace(',', '.'))
    df.loc[i, 'Minimo']=float(str(df.loc[i, 'Minimo']).replace('.','').replace(',', '.'))





df.to_csv('Archivos filtrados Pandas/Ethereum.csv', index=False)