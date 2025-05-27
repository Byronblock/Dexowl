import os
import time
from datetime import datetime, timedelta
from glob import glob

from utils.log_kit import logger
from config import klines_path

def check_data_update_flag(run_time, chain_name):
    """
    检查flag
    :param run_time:    当前的运行时间
    """
    flag_path = klines_path / chain_name / 'flags'
    max_flag = sorted(glob(os.path.join(flag_path, '*.flag')))
    if max_flag:
        max_flag_time = datetime.strptime(max_flag[-1].split(os.sep)[-1].split('.')[0], '%Y-%m-%d_%H_%M')
    else:
        max_flag_time = datetime(2000, 1, 1)  # 设置一个很早的时间，防止出现空数据

    index_file_path = os.path.join(flag_path, f"{run_time.strftime('%Y-%m-%d_%H_%M')}.flag")  # 构建本地flag文件地址
    while True:
        time.sleep(1)
        # 判断该flag文件是否存在
        if os.path.exists(index_file_path):
            flag = True
            break

        if max_flag_time < run_time - timedelta(minutes=30):  # 如果最新数据更新时间超过30分钟，表示数据中心进程可能崩溃了
            logger.error(f'数据中心进程疑似崩溃，最新数据更新时间：{max_flag_time}，程序启动时间：{run_time}')

        # 当前时间是否超过run_time
        if datetime.now() > run_time + timedelta(
                minutes=5):  # 如果当前时间超过run_time半小时，表示已经错过当前run_time的下单时间，可能数据中心更新数据失败，没有生成flag文件
            flag = False
            logger.warning(f"上次数据更新时间:【{max_flag_time}】，程序启动时间：【{run_time}】， 当前时间:【{datetime.now()}】")
            break

    return flag