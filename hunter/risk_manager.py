"""
风险管理模块
实现止盈止损功能
"""
from utils.log_kit import logger

def check_take_profit(df, token_info, quote_coin_price):
    """
    检查是否需要止盈
    当价格达到入场价格的2倍时，平仓一半
    """
    # 如果已经标记为止盈，则不再处理
    if token_info['take_profit']:
        return df
    
    # # 计算止盈价格
    actual_entry_price = float(token_info['entry_price']) * quote_coin_price
    take_profit_price = actual_entry_price * 2
    
    # 检查是否触发止盈
    if float(df['close'].values[0]) >= take_profit_price:
        df['take_profit'] = True
        df['signal'] = -1
        logger.info(f"{token_info['symbol']} 触发止盈 - 当前价格: {float(df['close'].values[0])} 入场价格: {actual_entry_price}")

    return df

def check_stop_loss(df, token_info, quote_coin_price):
    """
    检查是否需要止损
    当价格达到入场价格的0.5倍时，全部平仓
    """
    # 计算止损价格
    actual_entry_price = float(token_info['entry_price']) * quote_coin_price
    stop_loss_price = actual_entry_price * 0.6
    current_price = float(df['close'].values[0])
    
    # 检查是否触发止损
    if current_price < stop_loss_price:
        df['signal'] = -1
        df['stop_loss'] = True
        logger.info(f"{token_info['symbol']} 触发止损 - 当前价格: {current_price} 入场价格: {actual_entry_price}")

    return df
