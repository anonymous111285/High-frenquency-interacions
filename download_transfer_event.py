import requests
import pandas as pd
import os
from multiprocessing import Pool
from utlis import get_directories
from config import api_key, data_path

os.environ['HTTP_PROXY'] = 'http://127.0.0.1:12345'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:12345'


# 获取erc20转账log
def get_erc20_transfer_logs(api_key, address, startblock, endblock):
    i=1
    offset = 1000
    res_len = offset
    res_arr = []
    while res_len >= offset:
        url = f'https://api.etherscan.io/api?module=account&action=tokentx&address={address}&startblock={startblock}&endblock={endblock}&page={i}&offset={offset}&apikey={api_key}'
        r = requests.get(url)
        result = r.json()["result"]
        # print(i, len(result))
        res_len = len(result)
        res_arr += result
        i += 1
    return res_arr

def download_one_contract_block(api_key, address, block):
    folder_path = f'{data_path}/{address}/logs/transfer'
    if os.path.exists(f'{folder_path}/{block}.csv') or os.path.exists(f'{data_path}/{address}/logs/empty_transfer/{block}'):
        return
    result = get_erc20_transfer_logs(api_key, address, block, block)
    # if result == []:
    #     os.makedirs(f'{data_path}/{address}/logs/empty_transfer', exist_ok=True)
    #     os.makedirs(f'{data_path}/{address}/logs/empty_transfer/{block}', exist_ok=True)
    # else:
    df = pd.DataFrame(result)
    os.makedirs(folder_path, exist_ok=True)
    df.to_csv(f'{folder_path}/{block}.csv', index=False)


def download_one_contract(api_key, address):
    block_list = pd.read_csv(f'{data_path}/{address}/suspicious_txs.csv')['blockNumber'].value_counts().index.tolist()
    print(address,len(block_list))

    for block in block_list:
        download_one_contract_block(api_key, address, block)

    # arg_list = [(api_key, address, block) for block in block_list]
    # with Pool(processes=4) as pl:
    #     pl.starmap(download_one_contract_block, arg_list)


if __name__ == '__main__':
    contract_list = get_directories(data_path)
    i = 0

    contract_list = contract_list[i:]

    njobs = 1
    if njobs == 1:
        for contract_address in contract_list:
            print(i)
            i += 1
            download_one_contract(api_key, contract_address)
    elif njobs > 1:
        arg_list = [(api_key, contract_address) for contract_address in contract_list]
        with Pool(processes=njobs) as pl:
            pl.starmap(download_one_contract, arg_list)
    else:
        print('error')
