import ccxt
from config import proxy

# 从币安获取现货价格
bn_client = ccxt.binance({
    'timeout': 30000,
    'proxies': proxy
})

def get_symbol_current_price(symbol):
    """
    获取指定symbol的现货价格
    """
    current_price = bn_client.fetch_ticker(symbol)['last']
    
    return float(current_price)


if __name__ == '__main__':
    print(get_symbol_current_price('SOL/USDT'))

