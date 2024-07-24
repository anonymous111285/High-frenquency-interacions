import requests
import pandas as pd
import os
import json
from web3 import Web3
from config import *

def get_directories(folder_path):
    directories = []  # 用于存储目录名称的列表
    # 遍历文件夹中的每一个项
    for entry in os.listdir(folder_path):
        # 拼接完整的文件路径
        full_path = os.path.join(folder_path, entry)
        # 判断这个路径是否为目录
        if os.path.isdir(full_path):
            directories.append(entry)
    return directories

# 获取合约 source codes 
def get_source_codes(api_key, address):
    url = f'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={api_key}'
    r = requests.get(url)
    result = r.json()["result"]
    return result

# 判断合约是否开源
def if_contract_verified(api_key, address):
    try:
        return (True in [i['ABI']=='Contract source code not verified' for i in get_source_codes(api_key, address)])
    except:
        return False

# 将给定的十六进制字符串扩展到固定长度
def pad_hex_string(hex_string, total_length=66):
    # 移除'0x'前缀
    stripped_hex = hex_string[2:]
    # 计算需要填充的零的数量
    padding_length = total_length - len(hex_string)
    # 创建新的填充后的十六进制字符串
    padded_hex = '0x' + '0' * padding_length + stripped_hex
    return padded_hex

# 将固定长度的十六进制字符串化简
def unpad_hex_string(hex_string):
    new_hex_str = hex(int(hex_string, 16))
    new_hex_str = new_hex_str.replace('0x', '0x').lower()
    return new_hex_str

# 获取合约 logs 
def get_logs_by_topics(api_key, topics, fromBlock, toBlock, if_one_try=False):
    option_url = ''
    if 'address' in topics.keys():
        option_url += '&address=' + topics['address']
    if 'topic0' in topics.keys():
        option_url += '&topic0=' + topics['topic0']
    if 'topic1' in topics.keys():
        option_url += '&topic0_1_opr=and&topic1=' + pad_hex_string(topics['topic1'])
    i=1
    offset = 1000
    res_len = offset
    res_arr = []
    while res_len >= offset:
        url = f'https://api.etherscan.io/api?module=logs&action=getLogs&fromBlock={fromBlock}&toBlock={toBlock}{option_url}&page={i}&offset={offset}&apikey={api_key}'
        r = requests.get(url)
        result = r.json()["result"]
        # print(i, len(result))
        res_len = len(result)
        res_arr += result
        i += 1
        if if_one_try:
            break
    return res_arr

def get_lp_pair_info(web3_node_url, contract_address):
    pool_token_pair_path = f'dex_data/lp_pair/{contract_address}.json'
    if os.path.exists(pool_token_pair_path):
        with open(pool_token_pair_path, 'r') as json_file:
            token_pair = json.load(json_file)
        token0_address = token_pair['token0_address']
        token1_address = token_pair['token1_address']
        lp_name = token_pair['lp_name']
    else:
        w3 = Web3(Web3.HTTPProvider(web3_node_url))
        contract_abi = [
            {
                "constant": True,
                "inputs": [],
                "name": "token0",
                "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                "payable": False,
                "stateMutability": "view",
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "token1",
                "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                "payable": False,
                "stateMutability": "view",
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "name",
                "outputs":[{"internalType":"string","name":"","type":"string"}],
                "payable": False,
                "stateMutability": "view",
                "type": "function"
            }
        ]
        contract = w3.eth.contract(address=Web3.toChecksumAddress(contract_address.lower()), abi=contract_abi)
        token0_address = contract.functions.token0().call()
        token1_address = contract.functions.token1().call()
        lp_name = contract.functions.name().call()
        with open(pool_token_pair_path, 'w') as json_file:
            json.dump({
                'token0_address':token0_address,
                'token1_address':token1_address,
                'lp_name':lp_name,
            }, json_file, indent=4)
    return token0_address, token1_address, lp_name

def get_erc20token_info(web3_node_url, contract_address):
    token_address_path = f'dex_data/token/{contract_address}.json'
    if os.path.exists(token_address_path):
        with open(token_address_path, 'r') as json_file:
            token_json = json.load(json_file)
        symbol = token_json['symbol']
        decimals = token_json['decimals']
    else:
        w3 = Web3(Web3.HTTPProvider(web3_node_url))
        contract_abi = [
            {
                "constant": True,
                "inputs": [],
                "name": "symbol",
                "outputs":[{"internalType":"string","name":"","type":"string"}],
                "payable": False,
                "stateMutability": "view",
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs":[{"internalType":"uint8","name":"","type":"uint8"}],
                "payable": False,
                "stateMutability": "view",
                "type": "function"
            }
        ]
        contract = w3.eth.contract(address=Web3.toChecksumAddress(contract_address.lower()), abi=contract_abi)
        symbol = contract.functions.symbol().call()
        decimals = contract.functions.decimals().call()
        with open(token_address_path, 'w') as json_file:
            json.dump({
                'symbol':symbol,
                'decimals':decimals,
            }, json_file, indent=4)
    return symbol, int(decimals)


# 筛选uniswapV2的swap事件log
def get_uniV2_swap_logs(api_key, topics, fromBlock, toBlock):
    topics['topic0'] = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
    logs = get_logs_by_topics(api_key, topics, fromBlock, toBlock, if_one_try=True)
    df = pd.DataFrame(logs)
    if df.empty:
        return df
    df['sender'] = df['topics'].apply(lambda x: unpad_hex_string(x[1]))
    df['to'] = df['topics'].apply(lambda x: unpad_hex_string(x[2]))
    df['amount0In'] = df['data'].apply(lambda x: unpad_hex_string(x[2:2+64]))
    df['amount1In'] = df['data'].apply(lambda x: unpad_hex_string(x[2+64:2+64*2]))
    df['amount0Out'] = df['data'].apply(lambda x: unpad_hex_string(x[2+64*2:2+64*3]))
    df['amount1Out'] = df['data'].apply(lambda x: unpad_hex_string(x[2+64*3:2+64*4]))
    return df

def get_swap_event(api_key, block):
    log_path = f'dex_data/swap/{block}.csv'
    if os.path.exists(log_path):
        return pd.read_csv(log_path)
    df = get_uniV2_swap_logs(api_key, {}, block, block)
    df.to_csv(log_path, index=False)
    return df