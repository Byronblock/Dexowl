'''
信号说明：
    1代表开仓
    0代表平仓
'''
import numpy as np

def signal(df, *args):
    """
    :param df: 原始数据
    :param *args: signal计算的参数
    
    :return: 返回包含signal的数据

    布林带策略:
        价格突破上轨做多
        价格跌破中轨平仓
    """
    n = args[0]  # 布林带周期
    k = args[1]  # 布林带宽度系数
    
    # ===== 计算指标
    # 计算中轨（简单移动平均线）
    df['middle'] = df['close'].rolling(n, min_periods=1).mean()
    # 计算标准差
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof=0 表示总体标准差
    # 计算上轨、下轨
    df['upper'] = df['middle'] + k * df['std']
    df['lower'] = df['middle'] - k * df['std']

    # ===== 找出交易信号
    # === 找出开仓信号
    condition1 = df['close'] > df['upper']
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)
    df.loc[condition1 & condition2, 'signal'] = 1  # 将产生开仓信号的那根K线的signal设置为1，1代表开仓

    # === 找出平仓信号
    condition1 = df['close'] < df['middle']
    condition2 = df['close'].shift(1) >= df['middle'].shift(1)
    df.loc[condition1 & condition2, 'signal'] = -1  # 将产生平仓信号当天的signal设置为-1，-1代表平仓

    # ===== 合并信号，去除重复信号
    # === 去除重复信号
    temp = df[df['signal'].notnull()][['signal']]  # 筛选signal不为空的数据，并另存一个变量
    temp = temp[temp['signal'] != temp['signal'].shift(1)]  # 筛选出当前周期与上个周期持仓信号不一致的，即去除重复信号
    df['signal'] = temp['signal']  # 将处理后的signal覆盖到原始数据的signal列

    # 保留计算指标列，方便调试
    # df.drop(['middle', 'std', 'upper', 'lower'], axis=1, inplace=True)

    return df 