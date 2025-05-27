"""
Dexowl 主程序
根据配置文件遍历不同账户，并定期执行信号生成和交易操作
"""
import time
import traceback
import warnings
warnings.filterwarnings('ignore')

from config import accounts_info, interval_config, trade_config
from utils.commons import sleep_until_run_time, send_wechat_message
from utils.log_kit import logger, divider
from utils.datatools import check_data_update_flag
from hunter.position import active_position_process, active_pool_process, record_positions, create_position_files
from hunter.trade import order_place

is_debug = False

# 获取可交易的链列表
tradable_chains = [chain for chain, config in trade_config.items() if config['status']]
logger.ok(f"当前配置可交易的链: {tradable_chains}")

def main():
    """
    交易工作线程
    定期执行信号生成和交易操作
    """ 

    # 等待下一个合适的时间点
    random_seconds = 0  # 可以根据需要调整这个值，避免K线刚更新就执行
    if is_debug:
        run_time = sleep_until_run_time(interval_config['kline_interval'], if_sleep=False, cheat_seconds=random_seconds)
    else:
        run_time = sleep_until_run_time(interval_config['kline_interval'], if_sleep=True, cheat_seconds=random_seconds)

    # 按账户分组执行交易，每个账户对应其链上的交易
    for account_id, account_info in accounts_info.items():
        chain_name = account_info['strategy']['chain_name']
        
        # 检查该链是否可交易
        if chain_name not in tradable_chains:
            logger.warning(f"账户 {account_id} 所在链 {chain_name} 不可交易，跳过")
            continue
        
        # 等待该链的K线flag是否就绪
        if not is_debug:
            check_data_update_flag(run_time, chain_name)
        logger.info(f"开始处理账户 {account_id} 的仓位")
        
        # step1: 处理活跃仓位，获取卖出订单
        sell_orders_df = active_position_process(account_id, account_info, run_time)
        logger.info(f"活跃仓位处理完成")
        
        # step2: 处理活跃池子，获取买入订单
        buy_orders_df = active_pool_process(account_id, account_info, run_time)
        logger.info(f"活跃池子处理完成")

        # step3: 执行下单
        order_results = order_place(sell_orders_df, buy_orders_df, chain_name, account_info, account_id)
        logger.info(f"执行下单完成")

        # step4: 更新当前仓位和历史仓位
        record_positions(order_results, account_id, account_info)
        logger.info(f"仓位记录完成")

        logger.info(f"账户 {account_id} 处理完成")
    
    # 短暂休息，避免过度占用CPU
    logger.info("休息10秒后进入下一循环")
    time.sleep(10)
    

if __name__ == "__main__":
    
    # 确保数据目录结构
    create_position_files()   
    logger.info("数据目录结构初始化完成")    
    
    while True:
        try:
            main()
        except KeyboardInterrupt:
            logger.info("接收到停止信号，交易系统正在停止...")
            break  # 添加break确保正常退出
        except Exception as e:
            err_msg = f"主线程异常: {e}\n{traceback.format_exc()}"
            logger.error(err_msg)
            send_wechat_message(f"交易系统主线程异常: {e}")
            # 异常后等待一分钟再重试
            time.sleep(60)
    
    logger.info("交易系统已停止")