import pandas as pd
import json
import os
from multiprocessing import Pool
from utlis import get_directories
from config import *

def get_block_token_change(address):
    print(address)
    suspicious_txs = pd.read_csv(f'{data_path}/{address}/suspicious_txs.csv')
    if suspicious_txs.empty:
        try:
            os.remove(f'{data_path}/{contract_address}/token_change.csv')
        except:
            pass
        return
    # if os.path.exists(f'{data_path}/{contract_address}/token_change.csv'):
    #     return
    block_list = suspicious_txs['blockNumber'].value_counts().index.tolist()
    token_change = []
    for block in block_list:
        try:
            df = pd.read_csv(f'{data_path}/{address}/logs/transfer/{block}.csv')
            for index, row in df.iterrows():
                df.loc[index, 'bal'] = int(row['value'])/10**int(row['tokenDecimal'])
            token_bal = {}
            for token in df['contractAddress'].value_counts().index.tolist():
                send_bal = df[(df['from']==address)&(df['contractAddress']==token)]['bal'].sum()
                recive_bal = df[(df['to']==address)&(df['contractAddress']==token)]['bal'].sum()
                token_bal[token] = recive_bal - send_bal
            token_bal['block'] = block
            token_change.append(token_bal)
        except:
            continue
    df = pd.DataFrame(token_change).fillna(0)
    df = df.loc[:, (df != 0).any(axis=0)]
    if df.empty:
        try:
            os.remove(f'{data_path}/{contract_address}/token_change.csv')
        except:
            pass
        return
    else:
        df = df.set_index('block')
        df.to_csv(f'{data_path}/{contract_address}/token_change.csv')

if __name__ == '__main__':
    contract_list = get_directories(data_path)# ['0xb10E56Edb7698C960f1562c7edcC15612900c4A5'.lower(),'0xE8c060F8052E07423f71D445277c61AC5138A2e5'.lower()]#
    print(len(contract_list))

    njobs = 1

    if njobs == 1:
        for contract_address in contract_list:
            get_block_token_change(contract_address)
    else:
        with Pool(processes=njobs) as pl:
            pl.starmap(get_block_token_change, [[contract_address] for contract_address in contract_list])