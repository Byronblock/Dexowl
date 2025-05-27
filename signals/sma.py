'''
信号说明：
    1代表开仓
    0代表平仓
'''
def signal(df, *args):
    """
    :param df: 原始数据
    :param *args: siganl计算的参数
    
    :return: 返回包含signal的数据

    双均线策略:
        短线上穿长线做多
        短线下穿长线做空
    """
    n = args[0]
    m = args[1]
    # ===== 计算指标
    # 计算短线
    df['ma_short'] = df['close'].rolling(n,min_periods=1).mean()
    df['ma_long'] = df['ma_short'].rolling(m,min_periods=1).mean()

    # ===== 找出交易信号
    # === 找出开仓信号
    condition1 = df['ma_short'] > df['ma_long']
    condition2 = df['ma_short'].shift(1) <= df['ma_long'].shift(1)  
    df.loc[condition1 & condition2, 'signal'] = 1  # 将产生开仓信号的那根K线的signal设置为1，1代表开仓

    # === 找出做多平仓信号
    condition1 = df['ma_short'] < df['ma_long']
    condition2 = df['ma_short'].shift(1) >= df['ma_long'].shift(1)
    df.loc[condition1 & condition2, 'signal'] = -1  # 将产生平仓信号当天的signal设置为-1，-1代表平仓

    # ===== 合并做多做空信号，去除重复信号
    # === 去除重复信号
    temp = df[df['signal'].notnull()][['signal']]  # 筛选siganla不为空的数据，并另存一个变量
    temp = temp[temp['signal'] != temp['signal'].shift(1)]  # 筛选出当前周期与上个周期持仓信号不一致的，即去除重复信号
    df['signal'] = temp['signal']  # 将处理后的signal覆盖到原始数据的signal列

    # ===== 删除无关变量
    # df.drop(['ma_short','ma_long'], axis=1, inplace=True)  # 删除ma_short、ma_long列

    return df