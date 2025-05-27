"""
通用工具函数模块
"""
import time
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import traceback
import functools
from config import wechat_webhook_url, log_path
from utils.log_kit import logger

# ================== 重试功能 ==================
def retry(max_tries=3, delay_seconds=1, backoff=1, exceptions=(Exception,)):
    """
    重试装饰器，用于API调用重试
    :param max_tries: 最大尝试次数
    :param delay_seconds: 初始延迟秒数
    :param backoff: 退避系数
    :param exceptions: 需要捕获的异常
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            mtries, mdelay = max_tries, delay_seconds
            while mtries > 0:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    msg = f"{func.__name__} 调用失败: {str(e)}, 还剩 {mtries-1} 次重试"
                    logger.warning(msg)
                    
                    # 记录详细错误信息
                    logger.debug(f"错误详情: {traceback.format_exc()}")
                    
                    # 最后一次失败时抛出异常
                    if mtries == 1:
                        logger.error(f"{func.__name__} 达到最大重试次数，放弃重试")
                        raise
                    
                    # 等待一段时间再重试
                    time.sleep(mdelay)
                    
                    # 增加延迟时间
                    mdelay *= backoff
                    mtries -= 1
        return wrapper
    return decorator


# ================== 通知功能 ==================
def send_wechat_message(content, url=wechat_webhook_url):
    if not url:
        logger.warning('未配置wechat_webhook_url，不发送信息')
        return

    data = {
        "msgtype": "text",
        "text": {
            "content": content + '\n' + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    }
    response = requests.post(url, data=json.dumps(data), timeout=10)
    response.raise_for_status()
    logger.info('成功发送企业微信')




# ============= 运行时间管理 =============   
def next_run_time(time_interval, ahead_seconds=0):
    """
    根据time_interval，计算下次运行的时间。
    PS：目前只支持分钟和小时。
    :param time_interval: 运行的周期，15m，1h
    :param ahead_seconds: 预留的目标时间和当前时间之间计算的间隙
    :return: 下次运行的时间

    案例：
    15m  当前时间为：12:50:51  返回时间为：13:00:00
    15m  当前时间为：12:39:51  返回时间为：12:45:00

    10m  当前时间为：12:38:51  返回时间为：12:40:00
    10m  当前时间为：12:11:01  返回时间为：12:20:00

    5m  当前时间为：12:33:51  返回时间为：12:35:00
    5m  当前时间为：12:34:51  返回时间为：12:40:00

    30m  当前时间为：21日的23:33:51  返回时间为：22日的00:00:00
    30m  当前时间为：14:37:51  返回时间为：14:56:00

    1h  当前时间为：14:37:51  返回时间为：15:00:00
    """
    # 检测 time_interval 是否配置正确，并将 时间单位 转换成 可以解析的时间单位
    if time_interval.endswith('m') or time_interval.endswith('h'):
        pass
    elif time_interval.endswith('T'):  # 分钟兼容使用T配置，例如  15T 30T
        time_interval = time_interval.replace('T', 'm')
    elif time_interval.endswith('H'):  # 小时兼容使用H配置， 例如  1H  2H
        time_interval = time_interval.replace('H', 'h')
    else:
        logger.warning('time_interval格式不符合规范。程序exit')
        exit()

    # 将 time_interval 转换成 时间类型
    ti = pd.to_timedelta(time_interval)
    # 获取当前时间
    now_time = datetime.now()
    # 计算当日时间的 00：00：00
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    # 每次计算时间最小时间单位1分钟
    min_step = timedelta(minutes=1)
    # 目标时间：设置成默认时间，并将 秒，毫秒 置零
    target_time = now_time.replace(second=0, microsecond=0)

    while True:
        # 增加一个最小时间单位
        target_time = target_time + min_step
        # 获取目标时间已经从当日 00:00:00 走了多少时间
        delta = target_time - this_midnight
        # delta 时间可以整除 time_interval，表明时间是 time_interval 的倍数，是一个 整时整分的时间
        # 目标时间 与 当前时间的 间隙超过 ahead_seconds，说明 目标时间 比当前时间大，是最靠近的一个周期时间
        if int(delta.total_seconds()) % int(ti.total_seconds()) == 0 and int(
                (target_time - now_time).total_seconds()) >= ahead_seconds:
            break

    return target_time


def sleep_until_run_time(time_interval, ahead_time=1, if_sleep=True, cheat_seconds=120):
    """
    根据next_run_time()函数计算出下次程序运行的时候，然后sleep至该时间
    :param time_interval: 时间周期配置，用于计算下个周期的时间
    :param if_sleep: 是否进行sleep
    :param ahead_time: 最小时间误差
    :param cheat_seconds: 相对于下个周期时间，提前或延后多长时间， 100： 提前100秒； -50：延后50秒
    :return:
    """
    # 计算下次运行时间
    run_time = next_run_time(time_interval, ahead_time)
    # 计算延迟之后的目标时间
    target_time = run_time
    # 配置 cheat_seconds ，对目标时间进行 提前 或者 延后
    if cheat_seconds != 0:
        target_time = run_time - timedelta(seconds=cheat_seconds)
    logger.info(f'程序等待下次运行，下次时间：{target_time}')

    # sleep
    if if_sleep:
        # 计算获得的 run_time 小于 now, sleep就会一直sleep
        _now = datetime.now()
        if target_time > _now:  # 计算的下个周期时间超过当前时间，直接追加一个时间周期
            time.sleep(max(0, (target_time - _now).seconds))
        while True:  # 在靠近目标时间时
            if datetime.now() > target_time:
                time.sleep(1)
                break

    return run_time


def remedy_until_run_time(run_time):
    """
        使用了随机提前时间之后，需要补偿时间到run_time
    :param run_time: 需要补偿到达的时间点
    """
    # 如果设置提前下单，这里补偿一下时间
    _now = datetime.now()
    if _now < run_time:  # 当前时间比run_time时间要小，需要sleep到run_time时间
        time.sleep(max(0, (run_time - _now).seconds))
        while True:  # 当前时间逐渐靠近目标时间时
            if datetime.now() > run_time:
                time.sleep(1)
                break
            
# ============= 处理特殊字符 =============  
def replace_special_characters(symbol):
    """
    处理特殊字符
    """
    symbol = symbol.replace('/', '-').replace(':', '-').replace('*', '-').replace('?', '-').replace('\'', '-').replace(' ', '-').replace('"', '-')
    return symbol
