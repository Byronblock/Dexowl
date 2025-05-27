"""
K线获取模块
用于获取和更新K线数据
问题记录
1. 获取K线数据时，如果K线文件存在，但是最后一根K线距离当前时间太长，导致需要更新的K线非常多，就使用获取所有K线的方式
2. 有些代币的名称是非法文件字符，用replace_special_characters()函数处理，替换成"-"
"""
import os
import pandas as pd
import concurrent.futures
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from config import cmc_api_keys
import warnings
warnings.filterwarnings('ignore')

from config import root_path, interval_config, kline_min_count, klines_path, accounts_info
from clients.cmc_client import CMCClient
from utils.log_kit import logger
from utils.commons import replace_special_characters

# 创建CMC客户端
cmc_client = CMCClient(cmc_api_keys)

def collect_tokens_from_files(chain_name: str, account_id: str) -> pd.DataFrame:
    """
    从active_pool.csv和active_position.csv文件中收集代币信息
    
    chain_name: 链名称
    account_id: 账户ID
        
    Returns: 包含代币信息的DataFrame，主要字段：chain, address, symbol, pair_address
    """
    # 获取文件路径
    active_pool_path = root_path / 'data_feed' / f'{account_id}' / 'active_pool.csv'
    active_position_path = root_path / 'data_feed' / f'{account_id}' / 'active_position.csv'
    
    tokens_df = pd.DataFrame()
    
    # 读取active_pool.csv
    if active_pool_path.exists():
        # 检查文件是否为空
        if os.path.getsize(active_pool_path) > 0:
            pool_df = pd.read_csv(active_pool_path)
            
            if not pool_df.empty:
                # 选择需要的列
                if all(col in pool_df.columns for col in ['chain', 'address', 'symbol', 'pair_address']):
                    pool_df = pool_df[['chain', 'address', 'symbol', 'pair_address']]
                    tokens_df = pd.concat([tokens_df, pool_df])
                else:
                    logger.warning(f"{active_pool_path} 缺少必要列")
            else:
                logger.info(f"{account_id} 没有活跃池子")
        else:
            logger.info(f"{active_pool_path} 文件为空")

    
    # 读取active_position.csv
    if active_position_path.exists():
        # 检查文件是否为空
        if os.path.getsize(active_position_path) > 0:
            position_df = pd.read_csv(active_position_path)
            
            if not position_df.empty:
                # 选择需要的列
                if all(col in position_df.columns for col in ['chain', 'address', 'symbol', 'pair_address']):
                    position_df = position_df[['chain', 'address', 'symbol', 'pair_address']]
                    # 只选择status为'open'的记录
                    if 'status' in position_df.columns:
                        position_df = position_df[position_df['status'] == 'open']
                    tokens_df = pd.concat([tokens_df, position_df])
                else:
                    logger.warning(f"{active_position_path} 缺少必要列")
            else:
                logger.info(f"{account_id} 没有持仓")
        else:
            logger.info(f"{active_position_path} 文件为空")

    
    # 去重
    if not tokens_df.empty:
        tokens_df = tokens_df.drop_duplicates(subset=['address'])
        tokens_df.reset_index(drop=True, inplace=True)
        logger.info(f"账户{account_id}需要更新{len(tokens_df)}个代币的K线")
    else:
        logger.info(f"账户{account_id}没有代币需要更新K线")
    
    return tokens_df

def get_pair_address(token_address: str, chain_name: str) -> str:
    """
    获取代币的pair_address
    
    token_address: 代币地址
    chain_name: 链名称
        
    Returns: pair_address，如果获取失败则返回None
    """
    # 使用CMC客户端获取最大流动性的交易对
    pair_address, pair_name, liquidity, total_credit_count = cmc_client.get_pair_address_largest_liquidity(
        network_slug=chain_name,
        token_address=token_address
    )
    
    if pair_address:
        logger.ok(f"{token_address} 获取到交易对: {pair_name} - {pair_address}, 流动性: {liquidity}, credit_count: {total_credit_count}")
        return pair_address
    else:
        logger.warning(f"未找到{token_address}的交易对, credit_count: {total_credit_count}")
        return None
        


def update_active_pool_pair_address(chain_name: str, account_id: str, active_pool_df: pd.DataFrame) -> pd.DataFrame:
    """
    更新active_pool.csv文件中的pair_address字段
    
    chain_name: 链名称
    account_id: 账户ID
    active_pool_df: 包含代币信息的DataFrame
    Returns: 更新后的DataFrame
    """
    # 更新缺少pair_address的记录
    update_count = 0
    updated_df = active_pool_df.copy()
    
    for idx, row in active_pool_df.iterrows():
        if pd.isna(row['pair_address']) or not row['pair_address']:
            pair_address = get_pair_address(row['address'], chain_name)
            
            if pair_address:
                updated_df.loc[idx, 'pair_address'] = pair_address
                update_count += 1
    
    # 保存更新后的文件
    if update_count > 0:
        active_pool_path = root_path / 'data_feed' / f'{account_id}' / 'active_pool.csv'
        updated_df.to_csv(active_pool_path, index=False)
    
    return updated_df

def download_klines(token_data: Dict, chain_name: str, klines_dir: Path) -> bool:
    """
    下载单个代币的K线数据
    说明：
    1. 如果K线文件存在，则从K线文件中获取最后一条K线的开始时间
    2. 但是如果K线存在，最后一根K线距离时间太长，根据最小获取K线数量重现获取。
    3. 如果K线文件不存在，则获取最小K线数量

    token_data: 代币数据
    chain_name: 链名称
    klines_dir: K线保存目录
        
    Returns: 是否成功下载
    """
    token_address = token_data['address']
    token_symbol = token_data['symbol']
    pair_address = token_data['pair_address']
    
    if not pair_address or pd.isna(pair_address):
        logger.warning(f"代币{token_symbol} ({token_address})没有pair_address，跳过")
        return False
    
    # K线文件路径，处理特殊字符
    token_symbol = replace_special_characters(token_symbol)
    kline_file = klines_dir / f"{str(token_symbol)}_{str(token_address)}.csv"
    if kline_file.exists():
        klines_orign = pd.read_csv(kline_file)
        if not klines_orign.empty:
            start_time = klines_orign['candle_begin_time'].values[-1]
            
            # 判断是否需要获取所有K线
            # 计算当前时间与start_time的差值
            current_time = datetime.utcnow()
            start_datetime = pd.to_datetime(start_time)
            time_diff = current_time - start_datetime
            
            # 解析interval_config['kline_interval']
            interval = interval_config['kline_interval']
            minutes_per_interval = 0
            if interval.endswith('m'):
                minutes_per_interval = int(interval[:-1])
            elif interval.endswith('h'):
                minutes_per_interval = int(interval[:-1]) * 60
            
            # 计算需要更新的K线数量
            intervals_diff = time_diff.total_seconds() / (minutes_per_interval * 60)
            
            if intervals_diff > kline_min_count:
                # 如果更新K线的代币, 他上次更新已经很久了，导致需要更新的K线非常多，就使用获取所有K线
                logger.info(f"{token_symbol}上次更新时间为{start_time}，需要更新的K线数量({intervals_diff:.0f})超过最小K线获取数量({kline_min_count})，使用最少获取K线的方式")
                klines_df, total_credit_count = cmc_client.fetch_klines_df(
                    chain=chain_name,
                    contract_address=pair_address,
                    interval=interval_config['kline_interval'],
                    min_count=kline_min_count
                )
            else:
                # 只获取最新K线数据
                klines_df, total_credit_count = cmc_client.fetch_klines_df(
                    chain=chain_name,
                    contract_address=pair_address,
                    interval=interval_config['kline_interval'],
                    limit=50,
                    min_count=kline_min_count,
                    time_start=start_time
                )
            
            klines_df = pd.concat([klines_orign, klines_df]) # 合并K线数据
            klines_df.drop_duplicates(subset=['candle_begin_time'], inplace=True) # 去重
            klines_df.sort_values(by='candle_begin_time', inplace=True) # 排序
            klines_df.reset_index(drop=True, inplace=True) # 重置索引
    else:
        # 获取K线数据
        klines_df, total_credit_count = cmc_client.fetch_klines_df(
            chain=chain_name,
            contract_address=pair_address,
            interval=interval_config['kline_interval'],
            min_count=kline_min_count
        )
    
    if klines_df is None or klines_df.empty:
        logger.warning(f"未获取到{token_symbol}的K线数据, credit_count: {total_credit_count}")
        return False
    
    # 保存K线数据
    klines_df.to_csv(kline_file, index=False)
    logger.ok(f"获取到k线 - {token_symbol} : {token_address} - pair: {pair_address} - credit_count: {total_credit_count}")
    
    return True
    


def update_klines_for_chain(chain_name: str, account_ids: List[str], parallel: bool = True, max_workers: int = 3) -> int:
    """
    更新指定链上所有账户的K线数据
    
    chain_name: 链名称
    account_ids: 账户ID列表
    parallel: 是否并行处理
    max_workers: 最大工作线程数
        
    Returns: 成功更新的代币数量
    """
    # 创建K线目录
    klines_dir = root_path / 'data_feed' / 'klines' / chain_name
    klines_dir.mkdir(parents=True, exist_ok=True)
    
    flag_dir = klines_dir / 'flags'
    flag_dir.mkdir(parents=True, exist_ok=True)
    
    # 收集所有代币
    all_tokens = pd.DataFrame()
    for account_id in account_ids:
        # 获取该账户下的代币
        tokens_df = collect_tokens_from_files(chain_name, account_id)
        
        # 更新active_pool中缺少pair_address的记录
        active_pool_path = root_path / 'data_feed' / f'{account_id}' / 'active_pool.csv'
        if active_pool_path.exists():
            active_pool_df = pd.read_csv(active_pool_path)
            # 更新CSV文件中的pair_address并获取更新后的DataFrame
            updated_pool_df = update_active_pool_pair_address(chain_name, account_id, active_pool_df)
            
            # 使用更新后的DataFrame更新tokens_df中的pair_address
            if not tokens_df.empty and not updated_pool_df.empty:
                # 创建address到pair_address的映射
                address_to_pair = dict(zip(updated_pool_df['address'], updated_pool_df['pair_address']))
                
                # 更新tokens_df中的pair_address
                for idx, row in tokens_df.iterrows():
                    if row['address'] in address_to_pair:
                        tokens_df.loc[idx, 'pair_address'] = address_to_pair[row['address']]
        
        # 合并代币列表
        if not tokens_df.empty:
            all_tokens = pd.concat([all_tokens, tokens_df])
    
    # 去重
    if not all_tokens.empty:
        all_tokens = all_tokens.drop_duplicates(subset=['address'])
        all_tokens.reset_index(drop=True, inplace=True)
        
        # 过滤掉没有pair_address的代币
        valid_tokens = all_tokens[~all_tokens['pair_address'].isna() & (all_tokens['pair_address'] != '')]
        invalid_count = len(all_tokens) - len(valid_tokens)
        if invalid_count > 0:
            logger.warning(f"跳过{invalid_count}个没有pair_address的代币")
        
        all_tokens = valid_tokens
        logger.info(f"共收集到{len(all_tokens)}个有效代币，准备获取K线")
    else:
        logger.warning(f"未收集到任何有效代币，无法获取K线")
        return 0
    
    # 下载K线数据
    updated_count = 0
    
    if parallel and len(all_tokens) > 1:
        # 并行下载
        logger.info(f"使用并行模式下载K线数据，最大线程数: {max_workers}")
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(
                    download_klines,
                    token,
                    chain_name,
                    klines_dir
                ): idx for idx, token in all_tokens.iterrows()
            }
            
            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    success = future.result()
                    if success:
                        updated_count += 1
                    results.append(success)
                except Exception as e:
                    logger.error(f"处理代币索引{idx}时发生异常: {e}")
                    logger.error(traceback.format_exc())
                    results.append(False)
    else:
        # 顺序下载
        logger.info(f"使用顺序模式下载K线数据")
        for idx, token in all_tokens.iterrows():
            success = download_klines(token, chain_name, klines_dir)
            if success:
                updated_count += 1
    
    return updated_count

def create_flag(flag_dir: Path, run_time: datetime) -> None:
    """
    创建更新标志文件
    
    flag_dir: 标志文件目录
    run_time: 更新时间
    """
    # 添加更新标志
    flag_file = flag_dir / f"{run_time.strftime('%Y-%m-%d_%H_%M')}.flag"
    
    with open(flag_file, 'w', encoding='utf-8') as f:
        f.write('更新完成')
        f.close()
    
    # 清除旧的标志文件，只保留最新的100个
    all_flags = sorted(flag_dir.glob("*.flag"), key=os.path.getmtime, reverse=True)
    if len(all_flags) > 100:
        logger.info(f"清理旧的flag文件，删除{len(all_flags)-100}个")
        for old_flag in all_flags[100:]:
            old_flag.unlink()


def group_accounts_by_chain(accounts_info: Dict) -> Dict[str, List[str]]:
    """
    按链分组账户
    """
    chain_accounts = {}
    for account_id, account_info in accounts_info.items():
        chain_name = account_info['strategy']['chain_name']
        if chain_name not in chain_accounts:
            chain_accounts[chain_name] = []
        chain_accounts[chain_name].append(account_id)
    return chain_accounts

# ====================入口函数======================
def update_all_klines(run_time, parallel = False, max_workers = 3) -> Dict[str, int]:
    """
    更新所有链和账户的K线数据
    
    run_time: 更新时间
    parallel: 是否使用并发模式
    max_workers: 并发模式下的最大工作线程数
        
    Returns: 每条链更新的代币数量的字典
    """
    # 按链分组账户
    chain_accounts = group_accounts_by_chain(accounts_info)
    
    # 更新每条链的K线
    results = {}
    for chain_name, account_ids in chain_accounts.items():
        updated_count = update_klines_for_chain(chain_name, account_ids, parallel=parallel, max_workers=max_workers)
        # 创建更新完成标志文件
        flag_chain_path = klines_path / chain_name / 'flags'
        create_flag(flag_chain_path, run_time)
        results[chain_name] = updated_count            
    
    return results
