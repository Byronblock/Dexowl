"""
币池获取模块
用于获取和更新币池数据
"""
import pandas as pd
import time
import traceback
from datetime import datetime, timedelta
import queue
from typing import Dict
import warnings
warnings.filterwarnings('ignore')

from config import root_path, accounts_info, data_path, klines_path
from clients.gmgn_client import GMGNClient
from utils.log_kit import logger, divider

# 创建全局队列用于存放需要处理的池子
pool_queue = queue.Queue()
# 创建GMGN客户端实例，所有账户共用
gmgn_client = GMGNClient()

def update_active_pools(account_info):
    """
    更新活跃币池
    较少使用api获取pair_address，使用活跃池子、今天历史池子、昨天历史池子的pair_address
    """
    strategy_info = account_info['strategy']
    chain_name = strategy_info['chain_name']
    pool_config = strategy_info['pool_config']
    
    # 获取账户ID
    account_id = account_info['account_id']
    
    # 构建保存路径
    account_dir = root_path / 'data_feed' / f'{account_id}'
    account_dir.mkdir(parents=True, exist_ok=True)
    
    # 活跃池子文件路径，使用CSV格式
    active_pool_path = account_dir / 'active_pool.csv'
    
    # 创建地址-pair_address映射字典
    address_to_pair = {}
    
    # 检查是否存在原始active_pool.csv文件
    if active_pool_path.exists():
        # 读取原有的活跃池子数据
        old_pools_df = pd.read_csv(active_pool_path)
        
        # 提取address和pair_address映射关系
        for _, row in old_pools_df.iterrows():
            if pd.notna(row.get('pair_address')) and row['pair_address']:
                address_to_pair[row['address']] = row['pair_address']
    
    # 获取今天和昨天的日期
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    # 尝试从历史池子文件中读取pair_address
    # 今天的历史池子文件
    today_history_path = account_dir / 'history_pools' / f"history_pool_{today}.csv"
    if today_history_path.exists():
        today_history_df = pd.read_csv(today_history_path)
        # 提取address和pair_address映射关系
        for _, row in today_history_df.iterrows():
            if pd.notna(row.get('pair_address')) and row['pair_address']:
                if row['address'] not in address_to_pair:  # 不覆盖已有的映射
                    address_to_pair[row['address']] = row['pair_address']
    
    # 昨天的历史池子文件
    yesterday_history_path = account_dir / 'history_pools' / f"history_pool_{yesterday}.csv"
    if yesterday_history_path.exists():
        yesterday_history_df = pd.read_csv(yesterday_history_path)
        # 提取address和pair_address映射关系
        for _, row in yesterday_history_df.iterrows():
            if pd.notna(row.get('pair_address')) and row['pair_address']:
                if row['address'] not in address_to_pair:  # 不覆盖已有的映射
                    address_to_pair[row['address']] = row['pair_address']

    # 获取币池数据，直接使用全局客户端
    pools = gmgn_client.get_coins_pool(pool_config)
    
    if not pools:
        logger.warning(f"未获取到{chain_name}链的币池数据")
        return 0
    
    # 添加时间戳并处理pair_address
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for pool in pools:
        pool['update_time'] = current_time
        pool['chain'] = chain_name  # 确保每个池子记录包含链名称
        
        # 初始化pair_address为空字符串
        if 'pair_address' not in pool:
            pool['pair_address'] = ''
        
        # 如果地址在映射字典中存在，则保留原有的pair_address
        if pool['address'] in address_to_pair:
            pool['pair_address'] = address_to_pair[pool['address']]
        
    # 转换为DataFrame并保存为CSV
    pools_df = pd.DataFrame(pools)
    pools_df.to_csv(active_pool_path, index=False)
    
    # 将更新的池子放入队列，用于更新历史池子
    for pool in pools:
        pool_queue.put((chain_name, pool, datetime.now(), account_id))
    
    return len(pools)
    

def update_history_pools():
    """
    更新历史币池
    通过处理pool_queue中的池子，将其添加到当日的历史池子记录中
    """
    processed_count = 0
    
    while not pool_queue.empty():
        try:
            # 获取队列中的池子
            chain_name, pool, update_time, account_id = pool_queue.get(block=False)
            
            # 构建历史池子文件路径
            date_str = update_time.strftime("%Y-%m-%d")
            history_dir = root_path / 'data_feed' / f'{account_id}' / 'history_pools'
            history_dir.mkdir(parents=True, exist_ok=True)
            
            history_file = history_dir / f"history_pool_{date_str}.csv"
            
            # 准备池子数据，添加日期
            pool_data = pool.copy()
            pool_data['date'] = update_time.strftime("%Y-%m-%d")
            
            
            # 检查历史池子文件是否存在
            if history_file.exists():
                # 加载历史池子数据
                history_df = pd.read_csv(history_file)
                
                # 检查该池子是否已经存在于历史记录中
                existing = history_df[(history_df['id'] == pool_data['id']) & 
                                     (history_df['address'] == pool_data['address'])]
                
                if existing.empty:
                    # 池子不存在，添加到历史记录
                    pool_df = pd.DataFrame([pool_data])
                    history_df = pd.concat([history_df, pool_df], ignore_index=True)
                    
                    # 保存更新后的历史池子
                    history_df.to_csv(history_file, index=False)
                    logger.info(f"添加池子到历史记录: {pool_data['symbol']} : {pool_data['address']}")
            else:
                # 历史池子文件不存在，创建新文件
                pool_df = pd.DataFrame([pool_data])
                pool_df.to_csv(history_file, index=False)
            
            processed_count += 1
            
        except queue.Empty:
            break
        except Exception as e:
            logger.error(f"处理历史池子失败: {e}")
            logger.error(traceback.format_exc())
    
    return processed_count

def create_data_files():
    """
    确保数据目录存在
    """
    # 创建数据根目录
    data_path.mkdir(exist_ok=True)
    
    # 创建K线顶层目录
    klines_path.mkdir(exist_ok=True)
    
    # 为每个链创建K线子目录
    chain_set = set(account_info['strategy']['chain_name'] for account_info in accounts_info.values())
    for chain_name in chain_set:
        chain_klines_dir = klines_path / chain_name
        chain_klines_dir.mkdir(exist_ok=True)
        
        # 创建K线flags目录
        flags_dir = chain_klines_dir / 'flags'
        flags_dir.mkdir(exist_ok=True)
    
    # 为每个账户创建必要的目录结构
    for account_id, account_info in accounts_info.items():
        # 账户目录
        account_dir = data_path / f'{account_id}'
        account_dir.mkdir(exist_ok=True)
        
        # 历史池子目录
        history_pools_dir = account_dir / 'history_pools'
        history_pools_dir.mkdir(exist_ok=True)

# ====================入口函数======================
def update_all_pools(accounts_info: Dict) -> Dict[str, int]:
    """
    更新所有账户的币池数据
    accounts_info: 所有账户信息的字典
    """

    # 更新每个账户的池子
    for account_id, account_info in accounts_info.items():
        logger.info(f"正在更新账户 {account_id} 的币池数据")
        account_info_with_id = account_info.copy()
        account_info_with_id['account_id'] = account_id
        pools_count = update_active_pools(account_info_with_id)
        
        if pools_count:
            logger.info(f"账户 {account_id} 成功更新 {pools_count} 个币池")
        else:
            logger.warning(f"账户 {account_id} 未能更新币池数据")
        # 休息1秒，避免频率过高
        time.sleep(1)
    
    # 更新历史池子记录
    history_count = update_history_pools()
    logger.info(f"历史池子更新完成，处理了 {history_count} 条记录")

