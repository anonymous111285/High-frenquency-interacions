import requests
from tqdm import tqdm
import pandas as pd
import os
import time
from multiprocessing import Pool
from utlis import get_directories
from config import api_key, data_path

os.environ['HTTP_PROXY'] = 'http://127.0.0.1:12345'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:12345'


# 获取合约 txs 
def get_txs_by_address(api_key, contract_address, data_path):
    if os.path.exists(f'{data_path}/{contract_address}/origin_txs.csv'):
        res_arr = pd.read_csv(f'{data_path}/{contract_address}/origin_txs.csv')
        res_arr = res_arr.drop_duplicates(subset='hash')
        res_arr['blockNumber'] = res_arr['blockNumber'].astype(int)
        res_arr = res_arr.sort_values(by='blockNumber', ascending=True)
        startblock = res_arr['blockNumber'].values[-1]+1
    else:
        startblock=0
        res_arr = pd.DataFrame()
    offset = 10000
    res_len = offset
    while res_len >= offset:
        url = f'https://api.etherscan.io/api?module=account&action=txlist&address={contract_address}&startblock={startblock}&endblock=99999999&page=1&offset={offset}&sort=asc&apikey={api_key}'
        r = requests.get(url)
        result = r.json()["result"]
        res_len = len(result)
        time.sleep(1)
        if res_len == 0:
            break
        if type(result)==str:
            break
        startblock = int(result[-1]['blockNumber'])+1
        print(startblock, res_len)
        result = pd.DataFrame(result)
        res_arr = pd.concat([res_arr, result], axis=0, ignore_index=True)
        res_arr.to_csv(f'{data_path}/{contract_address}/origin_txs.csv', index=False)
    return res_arr

# 获取原始数据
def download_one_contract(api_key, contract_address):
    print(contract_address)
    contract_address = contract_address.lower()
    df = get_txs_by_address(api_key, contract_address, data_path)
    df.to_csv(f'{data_path}/{contract_address}/origin_txs.csv', index=False)


if __name__ == '__main__':
    contract_list = get_directories(data_path)
    # contract_list = ['0x000000005736775feb0c8568e7dee77222a26880']
    njobs = 1

    if njobs == 1:
        for contract_address in tqdm(contract_list):
            download_one_contract(api_key, contract_address)
    elif njobs > 1:
        arg_list = [(api_key, contract_address) for contract_address in contract_list]
        with Pool(processes=njobs) as pl:
            tqdm(pl.starmap(download_one_contract, arg_list), total=len(arg_list))
    else:
        print('error')
