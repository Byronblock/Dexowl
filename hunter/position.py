"""
仓位管理模块
处理活跃仓位和活跃池子的信号计算，决定交易策略
"""
import os
import time
import pandas as pd
import numpy as np
import concurrent.futures
from datetime import datetime
from pathlib import Path
import traceback
import warnings
warnings.filterwarnings('ignore')

from utils.log_kit import logger
from hunter.risk_manager import check_stop_loss, check_take_profit
from config import data_path, interval_config, accounts_info
from clients.bn_api import get_symbol_current_price
from utils.commons import send_wechat_message, replace_special_characters

# pandas相关的显示设置
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)

def create_position_files():
    """
    确保数据目录存在并初始化active_position.csv和history_positions目录
    """
    # 创建数据根目录
    data_path.mkdir(exist_ok=True)
    
    # 为每个账户创建必要的目录结构
    for account_id, account_info in accounts_info.items():
        # 账户目录
        account_dir = data_path / account_id
        account_dir.mkdir(exist_ok=True)
        
        # 创建历史仓位目录
        history_positions_dir = account_dir / 'history_positions'
        history_positions_dir.mkdir(exist_ok=True)
        
        # 创建active_position.csv文件（如果不存在）
        active_position_file = account_dir / 'active_position.csv'
        if not active_position_file.exists():
            # 创建一个包含必要字段的空DataFrame并保存为CSV
            columns = [
                'update_time', 'account_name', 'strategy', 'chain', 'symbol', 
                'address', 'pair_address', 'entry_time', 'exit_time', 'entry_price', 
                'exit_price', 'initial_amount', 'balance', 'quote_coin_symbol', 
                'quote_coin_amount', 'take_profit', 'status', 'pnl'
            ]
            empty_df = pd.DataFrame(columns=columns)
            empty_df.to_csv(active_position_file, index=False)
            logger.info(f"创建空的active_position.csv: {active_position_file}")


def calculate_signal(token_info, run_time, account_info):
    """
    计算交易信号
    
    Args:
        account_id: 账户ID
        token_info: 代币信息字典
        run_time: 运行时间
        
    Returns:
        信号结果: 
        1: 开仓
        -1: 平仓
        None: 无信号
    """
    symbol = token_info['symbol']
    address = token_info['address']
    signal_name, params = account_info['strategy']['signal_timing']
    chain = account_info['strategy']['chain_name']
    
    # 获取K线数据，处理特殊字符
    symbol = replace_special_characters(symbol)
    klines_file = data_path / f"klines/{chain}/{symbol}_{address}.csv"
    if not os.path.exists(klines_file):
        logger.warning(f"K线数据不存在: {klines_file}")
        return pd.DataFrame()
    
    # 读取K线数据
    df = pd.read_csv(klines_file, parse_dates=['candle_begin_time'])
    
    # 导入相应的策略模块
    signal_cls = __import__('signals.%s' % signal_name, fromlist=('',))
    
    # 计算信号
    df = signal_cls.signal(df, *params)
    
    # 将UTC时间转换为UTC+8
    df['candle_begin_time'] = df['candle_begin_time'] + pd.Timedelta(hours=8)
    
    # 获取最后一个有效信号
    df_signal = df.iloc[[-1]]
    
    # 检查信号时间是否在运行时间范围内
    run_time_pd = pd.to_datetime(run_time)
    time_diff = (run_time_pd - df_signal['candle_begin_time'].values[0]).total_seconds() / 60
    
    # 如果时间差大于一个周期（假设5分钟），则信号可能不是最新的
    if interval_config['kline_interval'].endswith('m'):
        interval = int(interval_config['kline_interval'][:-1])
    elif interval_config['kline_interval'].endswith('h'):
        interval = int(interval_config['kline_interval'][:-1]) * 60
    else:
        interval = 5

    if time_diff > interval * 2:
        logger.warning(f"{symbol} 信号时间 {df_signal['candle_begin_time']} 不在运行时间 {run_time_pd} 范围内")
        return pd.DataFrame()
    
    # 留下有用的列
    df_signal = df_signal[['candle_begin_time', 'symbol', 'signal', 'close', 'address', 'pair_address']]
    
    return df_signal


def active_position_process(account_id, account_info, run_time):
    """
    处理活跃仓位，获取卖出订单
    
    Args:
        account_id: 账户ID
        account_info: 账户信息
        
    Returns:
        卖出订单列表
    """
    
    # 获取活跃仓位文件路径
    active_position_file = data_path / account_id / 'active_position.csv'
    # 检查文件是否存在
    if not os.path.exists(active_position_file):
        logger.warning(f"活跃仓位文件不存在: {active_position_file}")
        return pd.DataFrame()
    
    # 读取活跃仓位
    active_positions = pd.read_csv(active_position_file)
    # 检查是否有活跃仓位
    if active_positions.empty:
        logger.info(f"账户 {account_id} 没有活跃仓位")
        return pd.DataFrame()

    # 过滤出活跃仓位
    active_df = active_positions[~active_positions['status'].isin(['closed', 'stop_loss'])]
    
    # 转换为字典列表
    token_infos = active_df.to_dict(orient='records')
    
    # 获取quote_coin的当前价格
    quote_coin_symbol = account_info['strategy']['quote_coin_symbol']
    quote_coin_price = get_symbol_current_price(f'{quote_coin_symbol}/USDT')
    
    sell_orders = pd.DataFrame()
    for token_info in token_infos:
        
        # 1.计算信号
        df = calculate_signal(token_info, run_time, account_info)
        
        # 2.检查止损
        df['stop_loss'] = False
        df = check_stop_loss(df, token_info, quote_coin_price)
        
        # 3.检查止盈, 新增一列take_profit
        df['take_profit'] = False
        df = check_take_profit(df, token_info, quote_coin_price)
        
        # 4.合并订单
        sell_orders = pd.concat([sell_orders, df], ignore_index=True)
        
    # 5.加上active_df的需要信息
    amount_info = active_df[['address', 'balance', 'pnl']]
    sell_orders = pd.merge(sell_orders, amount_info, on='address', how='left')
        
    logger.ok(f"活跃仓位订单:\n{sell_orders if not sell_orders.empty else 'No positions'}")
        
    return sell_orders
        

def active_pool_process(account_id, account_info, run_time):
    """
    处理活跃池子，获取买入订单
    account_id: 账户ID
    account_info: 账户信息
    run_time: 运行时间
    Returns: 买入订单列表
    """
    # 获取活跃池子
    active_pool_file = data_path / account_id / 'active_pool.csv'
    if not os.path.exists(active_pool_file):
        logger.warning(f"活跃池子文件不存在: {active_pool_file}")
        return pd.DataFrame()
    active_pool = pd.read_csv(active_pool_file)
    # 检查是否有活跃池子
    if active_pool.empty:
        logger.info(f"账户 {account_id} 没有活跃池子")
        return pd.DataFrame()

    # 活跃仓位的代币，不再开仓
    active_position_file = data_path / account_id / 'active_position.csv'
    if os.path.exists(active_position_file):
        active_position = pd.read_csv(active_position_file)    
    active_position_tokens = set(active_position['address'].unique())
    # 过滤已有仓位代币
    filtered_pool = active_pool[~active_pool['address'].isin(active_position_tokens)]
    
    if filtered_pool.empty:
        logger.info(f"账户 {account_id} 没有可开仓的新代币")
        return pd.DataFrame()

    # 转换为字典列表
    token_infos = filtered_pool.to_dict(orient='records')
    
    # 逐一计算信号
    buy_orders = pd.DataFrame()
    for token_info in token_infos:
        df = calculate_signal(token_info, run_time, account_info)
        buy_orders = pd.concat([buy_orders, df], ignore_index=True)
    
    logger.ok(f"活跃池子订单:\n{buy_orders if not buy_orders.empty else 'No hot coins'}")
    
    return buy_orders


def record_positions(order_results, account_id, account_info):
    """
    更新当前仓位和历史仓位
    order_results: 订单执行结果列表
    account_id: 账户ID
    account_info: 账户信息
    """
    if not order_results:
        return
    
    logger.info(f"开始更新账户 {account_id} 的仓位")
    
    # 获取活跃仓位文件路径
    active_position_file = data_path / account_id / 'active_position.csv'
    history_positions_dir = data_path / account_id / 'history_positions'
    
    # 读取活跃仓位
    if not os.path.exists(active_position_file):
        logger.warning(f"活跃仓位文件不存在: {active_position_file}")
        return
    active_positions = pd.read_csv(active_position_file)
    
    # 处理每个订单结果
    for result in order_results:
        if result['status'] != 'Success':
            continue
            
        current_time = datetime.now()
        
        # ================== 开仓处理 ==================
        if result['signal'] == 1:
            entry_price = (int(result['swap_from_amount']) / 10**9) / (int(result['swap_to_amount']) / 10**6)
            new_position = {
                'update_time': current_time,
                'account_name': account_id,
                'strategy': account_info['strategy']['strategy_name'],
                'chain': account_info['strategy']['chain_name'],
                'symbol': result['symbol'],
                'address': result['address'],
                'pair_address': result['pair_address'],
                'entry_time': result['execution_time'],
                'exit_time': None,
                'entry_price': entry_price,
                'exit_price': None,
                'initial_amount': int(result['swap_to_amount']),
                'balance': int(result['swap_to_amount']),
                'quote_coin_symbol': result['quote_coin_symbol'],
                'quote_coin_amount': int(result['swap_from_amount']),
                'take_profit': result['take_profit'],
                'status': 'open',
                'pnl': 0
            }
            
            # 添加到活跃仓位
            active_positions = pd.concat([active_positions, pd.DataFrame([new_position])], ignore_index=True)
            logger.ok(f"添加新仓位: {result['symbol']} - {result['address']}")
            

        # ================== 平仓处理 ==================
        elif result['signal'] == -1:
            
            # 获取当前仓位的信息
            mask = active_positions['address'] == result['address'] 
            if not any(mask):
                continue
            
            # 转为字典形式处理，更直观
            row_dict = active_positions[mask].iloc[0].to_dict()
            close_position = row_dict.copy()

            # ====== 止盈处理 ======
            if result['take_profit']:
                # pnl = (卖出的一半仓位的quote_coin数量 - quote_coin数量/2) / quote_coin精度
                if account_info['strategy']['chain_name'] == 'solana':
                    close_position['pnl'] = (int(result['swap_to_amount']) - int(close_position['quote_coin_amount']) / 2) / 10**9
                else:
                    pass                  
                close_position['update_time'] = current_time
                close_position['balance'] = int(close_position['balance']) - int(result['swap_from_amount'])
                close_position['take_profit'] = True

            # ====== 清仓处理 ======
            else:
                if account_info['strategy']['chain_name'] == 'solana':
                    exit_price = (int(result['swap_to_amount']) / 10**6) / (int(result['swap_from_amount']) / 10**9)
                    if close_position['take_profit']:
                        # 有止盈的情况下，pnl = (止盈时的pnl * quote_coin精度 + 本次收到的quote_coin数量 - 期初的quote_coin数量) / quote_coin精度
                        close_position['pnl'] = (close_position['pnl'] * 10**9 + int(result['swap_to_amount']) - int(close_position['quote_coin_amount'])) / 10**9
                    else:
                        # 没有止盈的情况下，pnl = 本次收到的quote_coin数量 / quote_coin精度
                        close_position['pnl'] = (int(result['swap_to_amount']) - int(close_position['quote_coin_amount']))/ 10**9
                else:
                    pass                
                close_position['update_time'] = current_time
                close_position['exit_time'] = result['execution_time']
                close_position['exit_price'] = exit_price
                close_position['balance'] = 0
                close_position['status'] = 'closed' if not result['stop_loss'] else 'stop_loss'

            # 更新活跃仓位
            active_positions = pd.concat([
                active_positions[~mask], 
                pd.DataFrame([close_position])
            ], ignore_index=True)
                
            # 添加到历史仓位
            history_position = close_position.copy()
            history_file = history_positions_dir / f"history_position_{current_time.strftime('%Y-%m-%d')}.csv"
            if os.path.exists(history_file):
                history_df = pd.read_csv(history_file)
                history_df = pd.concat([history_df, pd.DataFrame([history_position])], ignore_index=True)
                history_df.to_csv(history_file, index=False)
            else:
                pd.DataFrame([history_position]).to_csv(history_file, index=False)
        
        # 过滤掉已关闭的仓位, 并保存
        active_positions = active_positions[active_positions['status'] == 'open']
        active_positions.to_csv(active_position_file, index=False)
        
        # 发送企业微信
        if result['signal'] == 1:
            message = (
                f"🚀开仓成功!\n"
                f"代币: {new_position['symbol']} \n"
                f"地址: {new_position['address']} \n"
                f"价格: {new_position['entry_price']} \n"
                f"数量: {new_position['initial_amount'] / 10**6} \n"
                f"消耗: {new_position['quote_coin_amount'] / 10**9} SOL \n"
                f"链接: https://solscan.io/tx/{result['signature']}"
            )
        elif result['take_profit']:
            message = (
                f"😎止盈成功!\n"
                f"代币: {result['symbol']} \n"
                f"地址: {result['address']} \n"
                f"卖出数量: {int(result['swap_from_amount']) / 10**6} \n"
                f"回收: {int(result['swap_to_amount']) / 10**9} SOL \n"
                f"卖出收益: {close_position['pnl']} \n"
                f"卖出链接: https://solscan.io/tx/{result['signature']}"
            )
        elif result['stop_loss']:
            message = (
                f"😭止损了!\n"
                f"代币: {result['symbol']} \n"
                f"地址: {result['address']} \n"
                f"价格: {exit_price} \n"
                f"卖出数量: {int(result['swap_from_amount']) / 10**6} \n"
                f"回收: {int(result['swap_to_amount']) / 10**9} SOL \n"
                f"亏损: {close_position['pnl']} \n"
                f"卖出链接: https://solscan.io/tx/{result['signature']}"
            )
        else:
            message = (
                f"🤑清仓了!\n"
                f"代币: {result['symbol']} \n"
                f"地址: {result['address']} \n"
                f"价格: {exit_price} \n"
                f"卖出数量: {int(result['swap_from_amount']) / 10**6} \n"
                f"回收: {int(result['swap_to_amount']) / 10**9} SOL \n"
                f"总收益: {close_position['pnl']} \n"
                f"卖出链接: https://solscan.io/tx/{result['signature']}"
            )    

        send_wechat_message(message)
