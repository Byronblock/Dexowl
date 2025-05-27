"""
交易模块
封装了调用Jupiter API进行代币交易的功能
"""
import os
from datetime import datetime

from clients.jupiter_client import JupiterClient
from utils.log_kit import logger
from config import trade_config
from utils.commons import send_wechat_message

import pandas as pd
# pandas相关的显示设置
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)

def check_jupiter_signer():
    """
    检查Jupiter签名器是否存在
    """
    signer_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "clients", "jupiter_signer", "jupiter_signer.js")
    if not os.path.exists(signer_path):
        logger.error(f"Jupiter签名器不存在: {signer_path}")
        return False
    return True

def jupiter_place_order(order: dict, account_info, account_id, client):
    """
    执行交易
    order: {
        'candle_begin_time': '2024-01-01 00:00:00', # 信号时间
        'address': 'address', # 目标币地址
        'symbol': 'symbol', # 目标币符号
        'signal': 1, # 1: 买入, 0: 卖出
        
        # 以下为卖出订单
        'balance': 100, # 目标币余额
        'pnl': 100, # 目标币PnL
    }
    
    account_info: 账户信息
        
    Returns:
        交易结果
    """
    # 检查Jupiter签名器
    if not check_jupiter_signer():
        return {
            "status": "Failed",
            "error": "Jupiter签名器不存在",
            "order": order
        }    
    
    # 获取交易配置
    trade_params = trade_config['solana']
    slippage = trade_params['slippage'] * 10000  # 转换为基点, 0.01 = 100 slippage_bps
    
    # 根据交易类型决定输入和输出代币
    if order['signal'] == 1:
        # 买入: SOL -> 目标币
        input_mint = trade_params['quote_currency_address']  # 计价币地址
        output_mint = order['address']  # 目标币地址
        raw_amount = account_info['strategy']['position_size']
        amount = str(int(raw_amount * 10**9)) # 转换为SOL精度
        
        logger.ok(f"准备买入: {raw_amount} {trade_params['quote_currency']} -> {order['symbol']} - {order['address']}")
        
    elif order['signal'] == -1:
        # 卖出: 目标币 -> SOL
        input_mint = order['address']  # 目标币地址
        output_mint = trade_params['quote_currency_address']  # 计价币地址
        # 止盈卖一半
        if order['take_profit']:
            # balance就是大数字
            balance = int(order['balance'])
            amount = str(int(balance / 2))
            ui_amount = balance / 2 / 10**6
        else:
            amount = str(order['balance'])
            # 卖出: 目标币 -> SOL
            ui_amount = int(order['balance']) / 10**6
            
        logger.ok(f"准备卖出: {ui_amount} {order['symbol']}({order['address']}) -> {trade_params['quote_currency']}")

    # 一步执行交换操作
    result = client.swap(
        input_mint=input_mint,
        output_mint=output_mint,
        amount=amount,
        slippage_bps=int(slippage) if slippage else None
    )
            
    if result.get("status") == "Success":
        logger.ok(f"交易成功: https://solscan.io/tx/{result['signature']}")
        
        return {
            "status": "Success",
            
            # order的内容，用于更新仓位
            'signal': order['signal'],
            'symbol': order['symbol'],
            'address': order['address'],
            'pair_address': order['pair_address'],
            'quote_coin_symbol': 'SOL',
            'take_profit': order.get('take_profit', False),
            'stop_loss': order.get('stop_loss', False),
            
            # jupiter返回的内容
            "slot": result['slot'],
            "signature": result['signature'],
            "swap_from": input_mint,
            "swap_from_amount": result['inputAmountResult'],
            "swap_to": output_mint,
            "swap_to_amount": result['outputAmountResult'],
            
            "execution_time": datetime.now()
        }
    else:
        # 交易失败
        logger.error(f"交易失败: from {input_mint} to {output_mint} 错误原因: {result['error']} 错误代码: {result['code']} if result['code'] else ''")
        send_wechat_message(f"交易失败: from {input_mint} to {output_mint} 错误原因: {result['error']} 错误代码: {result['code'] if result['code'] else ''}")
        return {
            "status": "Failed",
            "error": result['error'],
            "slot": result['slot'],
            "execution_time": datetime.now()
        }
            


def order_place(sell_orders_df, buy_orders_df, chain_name, account_info, account_id):
    """
    执行下单
    
    Args:
        sell_orders: 卖出订单列表
        buy_orders: 买入订单列表
        
    Returns:
        执行结果列表
    """
    if sell_orders_df.empty and buy_orders_df.empty:
        logger.info("没有订单需要执行")
        return []
    
    results = []
    
    if chain_name == 'solana':
        place_order = jupiter_place_order
        # 初始化Jupiter客户端
        private_key = account_info.get('account_private_key')
        if not private_key:
            logger.error(f"{account_id} 账户私钥不存在")
            return []
        client = JupiterClient(public_key=account_info['account_address'], private_key=private_key)
    elif chain_name == 'bsc':
        # place_order = bsc_place_order
        pass

    # 先处理sell_orders_df
    if not sell_orders_df.empty:
        sell_orders_df = sell_orders_df[sell_orders_df['signal'] == -1] # 只保留卖出订单
        logger.critical(f"卖出订单:\n{sell_orders_df if not sell_orders_df.empty else 'WTF! NOTHING TO SELL!'}")
        sell_orders = sell_orders_df.to_dict(orient='records')
        for order in sell_orders:
            result = place_order(order, account_info, account_id, client)
            results.append(result)

    # 再处理buy_orders_df
    if not buy_orders_df.empty:
        buy_orders_df = buy_orders_df[buy_orders_df['signal'] == 1] # 只保留买入订单
        logger.critical(f"买入订单:\n{buy_orders_df if not buy_orders_df.empty else 'WTF! NOTHING TO BUY!'}")
        buy_orders = buy_orders_df.to_dict(orient='records')
        for order in buy_orders:
            result = place_order(order, account_info, account_id, client)
            results.append(result)

    
    logger.ok(f"执行完成 {len(results)} 个订单")
    return results


# ====== 待开发 ======
def jupiter_get_balance(account_info: dict):
    pass


def get_token_current_price(token_address, quote_address, jupiter_client):
    pass