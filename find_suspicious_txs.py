import pandas as pd
from multiprocessing import Pool
from utlis import get_directories
from config import data_path

def filter_groups(group):
    error_1 = group[group['isError'] == 1]['transactionIndex']
    error_0 = group[group['isError'] != 1]['transactionIndex']
    if error_1.empty or error_0.empty:
        return False
    return (error_1.max() > error_0.max()) and (error_1.max()-error_0.max() == 2)

# 筛选可疑交易
def filter_suspicious_txs(df):
    # 筛选有异常交易的区块
    df = df[df['blockNumber'].isin(df[df['isError']==1]['blockNumber'].tolist())]
    # 筛除去全部是异常交易的区块
    df = df[df.groupby('blockNumber')['isError'].transform('sum') != df.groupby('blockNumber')['isError'].transform('count')]
    # 筛选交易数量大于2的区块 
    value_counts = df['blockNumber'].value_counts()
    value_counts = value_counts[value_counts>=2]
    df = df[df['blockNumber'].isin(value_counts.index.tolist())]
    return df.groupby('blockNumber').filter(filter_groups)

def find_one_contract(contract_address):
    txs = pd.read_csv(f'{data_path}/{contract_address}/origin_txs.csv',low_memory=False)
    suspicious_txs = filter_suspicious_txs(txs)
    print(contract_address, len(suspicious_txs))
    suspicious_txs.to_csv(f'{data_path}/{contract_address}/suspicious_txs.csv', index=False)

if __name__ == '__main__':
    contract_list = ['0x00000000008C4FB1c916E0C88fd4cc402D935e7D'.lower()]#get_directories(data_path)#['0xb10E56Edb7698C960f1562c7edcC15612900c4A5'.lower(),'0xE8c060F8052E07423f71D445277c61AC5138A2e5'.lower()] # 
    njobs = 1

    if njobs == 1:
        for contract_address in contract_list:
            find_one_contract(contract_address)
    else:
        with Pool(processes=njobs) as pl:
            pl.starmap(find_one_contract, [[contract_address] for contract_address in contract_list])