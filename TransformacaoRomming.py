import os
import numpy as np
import pandas as pd
from LeituraParalela import LeituraParalela as lp


class TrasformarRomming(object):

    @staticmethod
    def transformarGS3(file_name, name):
        print("EXEC: Processando: "+file_name)
        # ignorar o bract
        if 'BRACT' in name:
            print("ERRO: Arquivo grande d mais")
            return

        # puxar o arquivo pelo panda
        df = pd.read_csv(file_name)

        # tirar a ultima linha
        df.drop(df.tail(1).index, inplace=True)

        # pegar arquivo q tenha dados
        if df.size > 0:
            ts_features_parallel_ppe = lp.parallel_feature_calculation_ppe(df, partitions=2, processes=1)
            print("EXEC: salvando em arquivo....")
            print(ts_features_parallel_ppe)
            if 'BRAC3' in name:
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAC3.csv", "w")
            elif 'BRAC4' in name:
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAC4.csv", "w")
            elif 'BRACT' in name:
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRACT.csv", "w")
            elif 'BRAI1' in name:
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAI1.csv", "w")
            elif 'BRAI2' in name:
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAI2.csv", "w")
            elif 'BRAI3' in name:
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAI3.csv", "w")
            else:
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/erro.csv", "w")

            arquivo.writelines(ts_features_parallel_ppe.to_csv())
            arquivo.close()
        else:
            print('ERRO: Arquivo muito pequeno ou muito grande')

    @staticmethod
    def atualizarMensal(dir_name_mensal, dir_name_diario, extension):
        
        os.chdir(dir_name_diario)
        for item in os.listdir(dir_name_diario):  # loop through items in dir
            if item.endswith(extension):  # check for ".csv" extension
                mensal_path = dir_name_mensal+'/mensal_'+item
                file_name = os.path.abspath(item)  # get full path of files
                if os.path.isfile(mensal_path):
                    TrasformarRomming.juntarMensalDiario(mensal_path, file_name)
                else:
                    diario = pd.read_csv(file_name)
                    mensal = open(mensal_path, 'w')
                    mensal.writelines(diario.to_csv(index=False))
                    mensal.close()

    @staticmethod
    def juntarMensalDiario(mensal_path, file_path):
        # Abre os arquivos diario e mensal
        print("EXE: juntando "+ file_path+ " com " + mensal_path)
        diario = pd.read_csv(file_path)
        mensal = pd.read_csv(mensal_path)	

        # soma o trafego e a qtd
        mensal.loc[(mensal['imsi'].isin(diario['imsi'])) & (mensal['operadora'].isin(diario['operadora'])), "sum"] += diario['sum']
        mensal.loc[(mensal['imsi'].isin(diario['imsi'])) & (mensal['operadora'].isin(diario['operadora'])), "count"] += diario['count']

        # concatena as instancias q estao em diario mas n estao em mensal
        mensal = pd.concat([mensal, diario]).drop_duplicates(subset=['imsi','operadora'])

        f = open(mensal_path, 'w')
        f.writelines(mensal.to_csv(index=False))
        f.close()

