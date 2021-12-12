import pandas as pd
from os import listdir
from os.path import isfile, join

#Modifica la estructura de las columnas numéricas para poder operar con ellas posteriormente.
onlyfiles = [f for f in listdir("./StaticFiles/PrimerProcesado") if isfile(join("./StaticFiles/PrimerProcesado", f))]

for k in onlyfiles:
    df=pd.read_csv(f"./StaticFiles/PrimerProcesado/{k}")

    for i in df.index:
        df.loc[i, 'Ultimo']=float(str(df.loc[i, 'Ultimo']).replace('.','').replace(',', '.'))
        df.loc[i, 'Apertura']=float(str(df.loc[i, 'Apertura']).replace('.','').replace(',', '.'))
        df.loc[i, 'Maximo']=float(str(df.loc[i, 'Maximo']).replace('.','').replace(',', '.'))
        df.loc[i, 'Minimo']=float(str(df.loc[i, 'Minimo']).replace('.','').replace(',', '.'))


    df.to_csv(f'./StaticFiles/SegundoProcesado/{k}', index=False)