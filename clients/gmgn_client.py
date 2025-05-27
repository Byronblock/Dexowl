"""
GMGN API客户端模块
用于获取GMGN币池数据
"""
import time
import pandas as pd
from curl_cffi import requests
import random
from config import proxy
from utils.commons import retry
from utils.log_kit import logger, divider

class GMGNClient:
    """
    GMGN API客户端
    """
    def __init__(self):
        """
        初始化GMGN客户端
        """
        self.base_url = 'https://gmgn.ai'
        self.proxy = proxy
        self.last_request_time = 0
        
        # 支持的浏览器指纹
        self.browsers = ["chrome131", "chrome124", "chrome116", "chrome119", "chrome120", "safari15_3", "safari15_5", "safari17_0", "safari17_2_ios", "edge99", "edge101"]

    def _get_headers(self):
        """
        获取请求头
        """
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",  
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://gmgn.ai/",  
            "Origin": "https://gmgn.ai",
            "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="131", "Chromium";v="131"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
    
    @retry(max_tries=3, delay_seconds=1)
    def _make_request(self, url, params=None):
        """
        发送API请求
        """
        self._respect_rate_limit()
        
        browser = random.choice(self.browsers)
        headers = self._get_headers()

        # 创建会话以保持状态
        s = requests.Session()
        if self.proxy:
            s.proxies = self.proxy
        
        # 先访问主页获取必要的cookie
        s.get("https://gmgn.ai/", impersonate=browser, timeout=30)
        
        # 然后请求目标API
        try:
            response = s.get(url, params=params, headers=headers, impersonate=browser, timeout=30)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"请求失败: {e}")
            return None
        
        return response.json()

        
    def _respect_rate_limit(self):
        """
        确保请求不超过速率限制
        """
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < 0.5:  # 限制每秒最多2个请求
            time.sleep(0.5 - elapsed)
        self.last_request_time = time.time()    
    
    def get_coins_pool(self, pool_config):
        """
        获取币池列表
        
        参数：
        pool_config (dict): 币池配置信息
            必须包含以下键：
            - chain: 链名称，如'sol'、'bsc'
            - type: 池子类型，如'hot'、'new'、'bluechip'
            - period: 时间周期，如'5m'、'1h'
            - order_types: 排序类型，如'volume'
            - order_directions: 排序方向，如'desc'
            - filters: 过滤条件列表
            - market_ranges: 市场范围限制字典
        """
        # 提取配置
        chain = pool_config['chain']
        pool_type = pool_config['type']
        period = pool_config['period']
        order_types = pool_config['order_types']
        order_directions = pool_config['order_directions']
        filters = pool_config['filters']
        market_ranges = pool_config['market_ranges']
        
        # url构建
        if pool_type == 'hot':
            url = f'{self.base_url}/defi/quotation/v1/rank/{chain}/swaps/{period}'
        elif pool_type == 'new':
            url = f'{self.base_url}/defi/quotation/v1/pairs/{chain}/new_pairs/{period}'
        elif pool_type == 'bluechip':
            url = f'{self.base_url}/api/v1/bluechip_rank/{chain}'
        else:
            logger.error(f"不支持的类型: {pool_type}")
            return []
        
        if not url:
            logger.error(f"不支持的链: {chain}")
            return []
        
        params = {
            "orderby": order_types,
            "direction": order_directions,
            "filters[]": filters,
            **market_ranges
        }
        
        result = self._make_request(url, params)
        if pool_type == 'hot':
            if not result or 'data' not in result or 'rank' not in result['data']:
                logger.error("API返回数据格式不符合预期")
                return []
            pool_data = result['data']['rank']
        elif pool_type == 'new':
            if not result or 'data' not in result or 'pairs' not in result['data']:
                logger.error("API返回数据格式不符合预期")
                return []
            pool_data = result['data']['pairs']
        elif pool_type == 'bluechip':
            if not result or 'data' not in result:
                logger.error("API返回数据格式不符合预期")
                return []
            pool_data = result['data']
        
        # 格式化数据
        formatted_coins = self._format_coin(pool_type, pool_data, chain)

        logger.info(f"{chain}链-{pool_type}池：获取到{len(formatted_coins)}个代币")
        return formatted_coins
    
    def _format_coin(self, pool_type, pool_data, chain):
        """
        格式化币池数据
        """
        formatted_coins = []
        if pool_type == 'hot' or pool_type == 'bluechip':
            for coin in pool_data:
                formatted_coin = {
                    "update_time": time.time(),
                    'pool_type': pool_type,
                    "id": coin.get("id", 0),
                    "chain": coin.get("chain", chain),
                    "pair_address": None,
                    "address": coin.get("address", ""),
                    "symbol": coin.get("symbol", ""),
                    "price": coin.get("price", 0),
                    "price_change_percent": coin.get("price_change_percent", 0),
                    "volume": coin.get("volume", 0),
                    "swaps": coin.get("swaps", 0),
                    "liquidity": coin.get("liquidity", 0),
                    "market_cap": coin.get("market_cap", 0),
                    "pool_creation_timestamp": coin.get("pool_creation_timestamp", 0),
                    "holder_count": coin.get("holder_count", 0),
                    "pool_type": coin.get("pool_type", 0),
                    "pool_type_str": coin.get("pool_type_str", ""),
                    "twitter_username": coin.get("twitter_username", None),
                    "website": coin.get("website", None),
                    "telegram": coin.get("telegram", None),
                    "total_supply": coin.get("total_supply", 0),
                    "open_timestamp": coin.get("open_timestamp", 0),
                    "price_change_percent1m": coin.get("price_change_percent1m", 0),
                    "price_change_percent5m": coin.get("price_change_percent5m", 0),
                    "price_change_percent1h": coin.get("price_change_percent1h", 0),
                    "buys": coin.get("buys", 0),
                    "sells": coin.get("sells", 0),
                    "initial_liquidity": coin.get("initial_liquidity", 0),
                    "is_show_alert": coin.get("is_show_alert", False),
                    "top_10_holder_rate": coin.get("top_10_holder_rate", None),
                    "renounced_mint": coin.get("renounced_mint", 0),
                    "renounced_freeze_account": coin.get("renounced_freeze_account", 0),
                    "burn_ratio": coin.get("burn_ratio", ""),
                    "burn_status": coin.get("burn_status", "unknown"),
                    "dev_token_burn_amount": coin.get("dev_token_burn_amount", None),
                    "dev_token_burn_ratio": coin.get("dev_token_burn_ratio", None),
                    "dexscr_ad": coin.get("dexscr_ad", 0),
                    "dexscr_update_link": coin.get("dexscr_update_link", 0),
                    "cto_flag": coin.get("cto_flag", 0),
                    "twitter_change_flag": coin.get("twitter_change_flag", 0),
                    "twitter_rename_count": coin.get("twitter_rename_count", 0),
                    "creator_token_status": coin.get("creator_token_status", "creator_hold"),
                    "creator_close": coin.get("creator_close", False),
                    "launchpad_status": coin.get("launchpad_status", 1),
                    "rat_trader_amount_rate": coin.get("rat_trader_amount_rate", 0),
                    "bluechip_owner_percentage": coin.get("bluechip_owner_percentage", 0),
                    "rug_ratio": coin.get("rug_ratio", None),
                    "sniper_count": coin.get("sniper_count", 5),
                    "smart_degen_count": coin.get("smart_degen_count", 0),
                    "renowned_count": coin.get("renowned_count", 0),
                    "is_wash_trading": coin.get("is_wash_trading", False),
                    
                    'initial_quote_reserve': None,
                    'bot_degen_count': None,
                    'launchpad': None,
                    'exchange': None,
                    'hot_level': None,
                    'social_links': None,
                    'creator': None,
                    'creator_created_inner_count': None,
                    'creator_created_open_count': None,
                    'creator_created_open_ratio': None,
                    'creator_balance_rate': None,
                    'bundler_trader_amount_rate': None,
                    'buy_tax': None,
                    'sell_tax': None,
                    'is_honeypot': None,
                    'renounced': None,
                }
                formatted_coins.append(formatted_coin)
        elif pool_type == 'new':
            for coin in pool_data:
                info = coin['base_token_info']
                formatted_coin = {
                    "update_time": time.time(),
                    'pool_type': pool_type,
                    "id": coin.get("id", 0),
                    "chain": chain,
                    "pair_address": coin.get("address", ""),
                    "address": info.get("address", ""),
                    "symbol": info.get("symbol", ""),
                    'price': info.get("price", 0),
                    'price_change_percent': None,
                    'volume': info.get("volume", 0),
                    'swaps': info.get("swaps", 0),
                    'liquidity': info.get("liquidity", 0),
                    'market_cap': info.get("market_cap", 0), 
                    'pool_creation_timestamp': coin.get("open_timestamp", 0),
                    "pool_type": coin.get("pool_type", 0),
                    "pool_type_str": coin.get("pool_type_str", ""),           
                    "twitter_username": info.get("social_links", {}).get("twitter_username", None),
                    "website": info.get("social_links", {}).get("website", None),
                    "telegram": info.get("social_links", {}).get("telegram", None),     
                    'total_supply': info.get("total_supply", 0),                                          
                    'holder_count': info.get("holder_count", 0),
                    'open_timestamp': coin.get("open_timestamp", 0),
                    'price_change_percent1m': info.get("price_change_percent1m", 0),
                    'price_change_percent5m': info.get("price_change_percent5m", 0),
                    'price_change_percent1h': info.get("price_change_percent1h", 0),
                    'buys': info.get("buys", 0),
                    'sells': info.get("sells", 0),               
                    'initial_liquidity': coin.get("initial_liquidity", 0),
                    'is_show_alert': info.get("is_show_alert", False),                    
                    'top_10_holder_rate': info.get("top_10_holder_rate", None),
                    'renounced_mint': info.get("renounced_mint", 0),
                    'renounced_freeze_account': info.get("renounced_freeze_account", 0),                    
                    'burn_ratio': info.get("burn_ratio", None),
                    'burn_status': info.get("burn_status", None),  
                    'dev_token_burn_amount': info.get("dev_token_burn_amount", None),
                    'dev_token_burn_ratio': info.get("dev_token_burn_ratio", None),                    
                    'dexscr_ad': info.get("dexscr_ad", 0),
                    'dexscr_update_link': info.get("dexscr_update_link", 0),                    
                    'cto_flag': info.get("cto_flag", None),                      
                    'twitter_change_flag': info.get("twitter_change_flag", None),
                    'twitter_rename_count': info.get("twitter_rename_count", None),
                    'creator_token_status': info.get("creator_token_status", "creator_hold"),
                    'creator_close': None,                    
                    'launchpad_status': None,
                    'rat_trader_amount_rate': info.get("rat_trader_amount_rate", None),                    
                    'bluechip_owner_percentage': info.get("bluechip_owner_percentage", None),
                    'rug_ratio': info.get("rug_ratio", None),                    
                    'sniper_count': info.get("sniper_count", None),
                    'smart_degen_count': info.get("smart_degen_count", None),
                    'renowned_count': info.get("renowned_count", None),
                    'is_wash_trading': info.get("is_wash_trading", None),
          
                    'initial_quote_reserve': coin.get("initial_quote_reserve", None),
                    'bot_degen_count': coin.get("bot_degen_count", None),
                    'launchpad': coin.get("launchpad", None),
                    'exchange': coin.get("exchange", None),
                    'hot_level': info.get("hot_level", None),
                    'social_links': info.get("social_links", None),
                    'creator': info.get("creator", None),
                    'creator_created_inner_count': info.get("creator_created_inner_count", None),
                    'creator_created_open_count': info.get("creator_created_open_count", None),
                    'creator_created_open_ratio': info.get("creator_created_open_ratio", None),
                    'creator_balance_rate': info.get("creator_balance_rate", None),
                    'bundler_trader_amount_rate': info.get("bundler_trader_amount_rate", None),
                    'buy_tax': info.get("buy_tax", None),
                    'sell_tax': info.get("sell_tax", None),
                    'is_honeypot': info.get("is_honeypot", None),
                    'renounced': info.get("renounced", None),
                                    
                }
                formatted_coins.append(formatted_coin)
        
        return formatted_coins

# 如果直接运行此文件，则执行测试
if __name__ == "__main__":
# 测试函数
    try:
        # 初始化客户端
        client = GMGNClient()
        
        # 测试配置
        test_config = {
            'chain': 'bsc',
            'type': 'new',
            'period': '5m',
            'order_types': 'volume',
            'order_directions': 'desc',
            'filters': ['not_honeypot', 'verified','renounced', 'locked', 'not_wash_trading'],
            'market_ranges': {
                'max_created': "50000m",
                'min_liquidity': 50000,
                'max_marketcap': 10000000,
            }
        }
        
        # 获取币池数据
        pools = client.get_coins_pool(test_config)
        
        if not pools:
            print("未获取到任何币池数据")
            exit()
        
        # 打印获取到的币池数量
        print(f"成功获取 {len(pools)} 个币池数据")
        df = pd.DataFrame(pools)
        # df.to_csv('gmgn_pools.csv', index=False)
        print(df)
            
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        exit()
    