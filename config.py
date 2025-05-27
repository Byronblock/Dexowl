"""
DEX选币-择时框架配置文件
"""
import os
from pathlib import Path

# 策略信息
accounts_info = {
    'account_1': {
        'account_address': '替换为你的钱包地址',
        'account_private_key': os.getenv('account_1_private_key'),
        'strategy': {
            'strategy_name': 'gmgn热门币均线策略',
            'signal_timing': ('sma', [5, 10]),
            'chain_name': 'solana',
            'quote_coin_symbol': 'SOL',
            'position_size': 0.01,
            # 币池配置
            'pool_config': {
                'data_source': 'gmgn',
                'chain': 'sol',
                'type': 'hot',
                'period': '5m',
                'order_types': 'volume',
                'order_directions': 'desc',
                'filters': ['has_social', 'not_wash_trading', 'renounced', 
                            'frozen', 'burn', 'distribed'],
                'market_ranges': {
                    'max_created': "50000m",
                    'min_liquidity': 50000,
                    'max_marketcap': 10000000,
                    'min_volume': 5000,
                }
            },            
        }
    },
    # 'account_1': {
    #     'account_address': '替换为你的钱包地址',
    #     'account_private_key': os.getenv('account_1_private_key'),
    #     'strategy': {
    #         'strategy_name': 'gmgn热门币均线策略',
    #         'signal_timing': ('sma', [5, 10]),
    #         'chain_name': 'solana',
    #         'quote_coin_symbol': 'SOL',
    #         'position_size': 0.01,
    #         # 币池配置
    #         'pool_config': {
    #             'data_source': 'gmgn',
    #             'chain': 'sol',
    #             'type': 'new',
    #             'period': '5m',
    #             'order_types': 'volume',
    #             'order_directions': 'desc',
    #             'filters': ['has_social', 'not_wash_trading', 'renounced', 
    #                         'frozen', 'burn', 'distribed'],
    #             'market_ranges': {
    #                 'min_created': "15m",
    #                 'max_created': "120m",
    #                 'max_marketcap': 1000000,
    #                 'min_volume': 200,
    #             }
    #         },            
    #     }
    # },    
    
    # 'account_2': {
    #     'account_address': '替换为你的钱包地址',
    #     'account_private_key': os.getenv('account_2_private_key'),
    #     'strategy': {
    #         'strategy_name': 'gmgn新币bolling策略',
    #         'signal_timing': ('bolling', [200, 2]),
    #         'chain_name': 'bsc',
    #         'quote_coin_symbol': 'BNB',
    #         'position_size': 0.1,
    #         # 币池配置
    #         'pool_config': {
    #             'data_source': 'gmgn',
    #             'chain': 'bsc',
    #             'type': 'hot',
    #             'period': '5m',
    #             'order_types': 'volume',
    #             'order_directions': 'desc',
    #             'filters': ['not_honeypot', 'verified', 'renounced', 
    #                         'locked', 'not_wash_trading'],
    #             'market_ranges': {
    #                 'max_created': "50000m",
    #                 'min_liquidity': 10000,
    #                 'max_marketcap': 10000000,
    #                 'min_volume': 1000,
    #             }
    #         },            
    #     }
    # }    
}

# 是否处于调试模式
is_debug = True

# 代理设置
proxy = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
# proxy = None

# cmc的密钥，可以配置多个
cmc_api_keys = [os.getenv('cmc_api_key_1')] 

# k线最小获取数量
kline_min_count = 14

# 间隔时间设置
interval_config = {
    'kline_interval': '5m',  # 交易K线间隔，单位m
}

# 交易参数
trade_config = {
    'solana': {
        'status': True,
        'slippage': 0,  # 滑点，设置为0时，使用jupiter的默认滑点
        # 'priority_fee': 0.01,  # 优先费率
        # 'mev_fee': None,  # mev费率, None为不使用mev
        'quote_currency': 'sol',
        'quote_currency_address': 'So11111111111111111111111111111111111111112',
    },
    'bsc': {
        'status': False,
        'slippage': 0.05,  # 滑点
        # 'priority_fee': 0.01,  # 优先费率
        # 'mev_fee': None,  # mev费率, None为不使用mev
        'quote_currency': 'wbnb',
        'quote_currency_address': '',
    },
}

# 项目根目录
root_path = Path(os.path.dirname(__file__))

# 数据目录结构
data_path = root_path / 'data_feed'
klines_path = data_path / 'klines'
log_path = root_path / 'logs'
cmc_api_stats_path = data_path / 'cmc_AIPStats'

# 钉钉设置
wechat_webhook_url = f'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={os.getenv("wechat_webhook_url")}'
