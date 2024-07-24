import pandas as pd
import os
import json
from config import *
from utlis import *
from multiprocessing import Pool


os.environ['HTTP_PROXY'] = 'http://127.0.0.1:12345'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:12345'




def get_uniV2_sync_logs(api_key, topics, fromBlock, toBlock):
    topics['topic0'] = '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1'
    logs = get_logs_by_topics(api_key, topics, fromBlock, toBlock)
    df = pd.DataFrame(logs)
    if df.empty:
        return df
    df['reserve0'] = df['data'].apply(lambda x: int(x[2:2+64], 16))
    df['reserve1'] = df['data'].apply(lambda x: int(x[2+64:2+64*2], 16))
    df['blockNumber'] = df['blockNumber'].apply(lambda x: int(x, 16))
    df['transactionIndex'] = df['transactionIndex'].apply(lambda x: int(pad_hex_string(x,16), 16))
    df['logIndex'] = df['logIndex'].apply(lambda x: int(x, 16))
    return df

def download_sync_event(api_key, block):
    log_path = f'dex_data/sync/{block}.csv'
    if os.path.exists(log_path):
        return
    df = get_uniV2_sync_logs(api_key, {}, block, block)
    if not df.empty:
        for pool_address in df['address'].value_counts().index:
            token0_address, token1_address, lp_name = get_lp_pair_info(web3_node_url, pool_address)
            _index = df[df['address']==pool_address].index
            df.loc[_index, 'token0_address'] = token0_address
            df.loc[_index, 'token1_address'] = token1_address
            df.loc[_index, 'lp_name'] = lp_name
    df.to_csv(log_path, index=False)

def download_swap_event(api_key, topics, fromBlock, toBlock):
    contract_address = topics['topic1']
    log_path = f'{data_path}/{contract_address}/logs/swap/{toBlock}.csv'
    if os.path.exists(log_path):
        return 
    df = get_uniV2_swap_logs(api_key, {}, fromBlock, toBlock)
    df.to_csv(log_path, index=False)
    print(contract_address,toBlock)

def process_one_contract(contract_address):
    if os.path.exists(f'{data_path}/{contract_address}/loss.csv'):
        loss = pd.read_csv(f'{data_path}/{contract_address}/loss.csv')
        txs = pd.read_csv(f'{data_path}/{contract_address}/origin_txs.csv')
        for index, row in loss.iterrows():
            download_sync_event(api_key, int(row['block']))

            reverted_txs = pd.read_csv(f'{data_path}/{contract_address}/suspicious_txs.csv')
            reverted_txs = reverted_txs[reverted_txs['blockNumber']==int(row['block'])]
            # reverted_txs = reverted_txs[reverted_txs['isError']==1]
            for _, tx in reverted_txs.iterrows():
                # input_data = tx['input']
                method_id = tx['methodId']
                same_method_txs = txs[txs['methodId']==method_id]
                block_list = same_method_txs['blockNumber'].tolist()[:100]
                for block in block_list:
                    download_swap_event(api_key, {'topic1':contract_address}, block, block)
                    # print(contract_address[:6], 'loss', index, len(loss),'block_list', block_list.index(block), len(block_list))

if __name__ == '__main__':
    contract_list = ['0x000000000000084e91743124a982076c59f10084', '0x00000000000080c886232e9b7ebbfb942b5987aa', '0x000000000035b5e5ad9019092c665357240f594e', '0x00000000003b3cc22af3ae1eac0440bcee416b40', '0x0000000000d41c96294ccdac8612bdfe29c641af', '0x000000000dfde7deaf24138722987c9a6991e2d4', '0x0000000099cb7fc48a935bceb9f05bbae54e8987', '0x000000e1fddf4fe15db5f23ae3ee83c6a11e8dd1', '0x1fb421310ceacd0afb2a429bbb4682e522b38ecb', '0x4a137fd5e7a256ef08a7de531a17d0be0cc7b6b6', '0x4d246be90c2f36730bb853ad41d0a189061192d3', '0x5050e08626c499411b5d0e0b5af0e83d3fd82edf', '0x51399b32cd0186bb32230e24167489f3b2f47870', '0x585c3d4da9b533c7e3df8ac7356c882859298cee', '0x70e86223507724bf2c51fe3ac2cc78c67bfad366', '0x8032eaede5c55f744387ca53aaf0499abcd783e5', '0x87d9da48db6e1f925cb67d3b7d2a292846c24cf7', '0x8aff5ca996f77487a4f04f1ce905bf3d27455580', '0x8d7f71c25fb581b7ba2efbb10345d262d1852ff5', '0x98c3d3183c4b8a650614ad179a1a98be0a8d6b8e', '0xa69babef1ca67a37ffaf7a485dfff3382056e78c', '0xb10e56edb7698c960f1562c7edcc15612900c4a5', '0xbeefbabeea323f07c59926295205d3b7a17e8638', '0xc758d5718147c8c5bc440098d623cf8d96b95b83', '0xe1f08d771fb7b248b3266b7f79a9eafba3147c2d', '0xe4000004000bd8006e00720000d27d1fa000d43e', '0xe8c060f8052e07423f71d445277c61ac5138a2e5', '0xf8b721bff6bf7095a0e10791ce8f998baa254fd0']    #get_directories(data_path) # ['0xE8c060F8052E07423f71D445277c61AC5138A2e5'.lower()] # 
    print(len(contract_list))
    njobs = 4

    if njobs == 1:
        for contract_address in contract_list:
            process_one_contract(contract_address)
    else:
        with Pool(processes=njobs) as pl:
            pl.starmap(process_one_contract, [[contract_address] for contract_address in contract_list])