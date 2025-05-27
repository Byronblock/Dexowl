"""
资金分配模块
决定开仓的金额和最大开仓数
"""
from utils.log_kit import logger, divider

def allocate_funds(buy_orders, sell_orders, account_id, account_info):
    """
    决定开仓的金额
    
    Args:
        buy_orders: 买入订单列表
        sell_orders: 卖出订单列表
        account_id: 账户ID
        account_info: 账户信息
        
    Returns:
        更新后的买入订单列表
    """
    # 如果没有买入订单，直接返回空列表
    if not buy_orders:
        return []
    
    # 获取账户配置信息
    position_size = account_info['strategy']['position_size']
    quote_currency = account_info['strategy']['quote_currency']
    
    # 获取当前活跃仓位数（假设卖出后仓位已经空出来了）
    current_active_positions = len(buy_orders) + len(sell_orders)
    
    # 设置最大持仓数量限制（可以从配置中读取，这里简单设置为5个）
    max_positions = 5
    
    logger.info(f"当前活跃仓位数: {current_active_positions}, 最大持仓数: {max_positions}")
    
    # 更新买入订单，添加资金分配信息
    updated_buy_orders = []
    positions_to_add = max_positions - current_active_positions
    
    if positions_to_add <= 0:
        logger.warning(f"已达到最大持仓数 {max_positions}，不再开新仓")
        return []
    
    # 限制新开仓位数量
    buy_orders_to_process = buy_orders[:positions_to_add]
    
    for order in buy_orders_to_process:
        # 设置固定开仓金额
        order['quote_amount'] = position_size
        order['quote_currency'] = quote_currency
        updated_buy_orders.append(order)
        
        logger.info(f"分配资金: {order['symbol']} 开仓金额 {position_size} {quote_currency}")
    
    return updated_buy_orders 