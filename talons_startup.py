"""
数据中心启动程序
根据配置文件遍历不同账户，初始化数据环境，并定期启动相关数据获取模块
"""
import time
import traceback
import warnings
warnings.filterwarnings('ignore')

from config import accounts_info, interval_config
from talons.pools_generator import update_all_pools, create_data_files
from talons.klines_fetcher import update_all_klines
from utils.commons import sleep_until_run_time, send_wechat_message
from utils.log_kit import logger, divider

is_debug = True


def main():
    # 等待下一个合适的时间点
    random_seconds = 0  # 可以根据需要调整这个值，避免K线刚更新就执行
    if is_debug:
        run_time = sleep_until_run_time(interval_config['kline_interval'], if_sleep=False, cheat_seconds=random_seconds)
    else:
        run_time = sleep_until_run_time(interval_config['kline_interval'], if_sleep=True, cheat_seconds=random_seconds)
    
    # 更新所有账户的池子
    start_time = time.time()
    update_all_pools(accounts_info)

    elapsed = time.time() - start_time
    logger.info(f"数据中心启动完成，耗时 {elapsed:.2f} 秒")
    
    # 更新K线
    start_time = time.time()
    results = update_all_klines(run_time, parallel=True if not is_debug else False, max_workers=3)
    
    # 计算总更新数量
    updated_total = sum(results.values())
    
    # 计算本次更新耗时
    elapsed = time.time() - start_time
    logger.info(f"本次K线更新完成，共更新 {updated_total} 个K线，耗时 {elapsed:.2f} 秒")
    
    # 短暂休息，避免过度占用CPU
    logger.info("休息10秒后进入下一循环")
    time.sleep(10)
    

if __name__ == "__main__":
    logger.info("数据中心启动")
    
    # 确保数据目录结构
    create_data_files()
    
    while True:
        try:
            main()
        except KeyboardInterrupt:
            logger.info("接收到停止信号，数据中心正在停止...")
            break  # 添加break确保正常退出
        except Exception as e:
            err_msg = f"主线程异常: {e}\n{traceback.format_exc()}"
            logger.error(err_msg)
            send_wechat_message(f"数据中心主线程异常: {e}")
            # 异常后等待一分钟再重试
            time.sleep(60)
    
    logger.info("数据中心已停止")    
