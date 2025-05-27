"""
CoinMarketCap API客户端模块（轻量版）
用于获取K线数据和交易对信息
cmc使用的时间是iso8601格式, UTC时间
cmc的API调用是用credit_count来计算的，1根k线就是1个credit_count，非常变态
所以请求k线不要太多，否则会爆
第一次请求会少一根k线

250421更新
- credit_count的精确计算方式
- 获取30根k线,就消耗30个credit_count
- 获取10个交易对,就消耗10个credit_count
- 一定要节约使用credit_count
"""
import time
import pandas as pd
import requests
import traceback
from datetime import datetime, timedelta

from config import cmc_api_stats_path
from utils.commons import send_wechat_message, retry
from utils.log_kit import logger


class CMCClient:
    """
    CoinMarketCap API客户端
    """
    def __init__(self, api_keys=[]):
        """
        api_key: 传入一个列表，列表中是API密钥
        """
        self.base_url = 'https://pro-api.coinmarketcap.com/v4'
        
        # api_key
        self.api_keys = api_keys
        self.main_key = self.api_keys[0]  # 第一个为主密钥
        self.backup_keys = self.api_keys[1:] if len(self.api_keys) > 1 else []
        self.api_key = None  # 当前使用的API密钥
        
        # 日志
        self.logger = logger
        
        # 速率限制
        self.rate_limit = {
            'per_minute': 295,
            'per_month': 990000
        }
        
        # 初始化统计文件路径
        self.stats_dir = cmc_api_stats_path
        self.stats_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载或初始化API使用记录
        self.usage_stats = self._load_usage_stats()
        
        # 记录当前月份，用于检测月份变更
        self.current_month = datetime.utcnow().strftime('%Y%m')
        
    # ============= API使用统计功能 =============
    def _load_usage_stats(self):
        """加载API使用统计"""
        current_utc = datetime.utcnow()
        current_month = current_utc.strftime('%Y%m')
        stats_file = self.stats_dir / f"api_usage_{current_month}.csv"
        
        if stats_file.exists():
            stats_df = pd.read_csv(stats_file)
            return {
                key: {
                    'minute_calls': 0,  # 当前分钟的调用次数
                    'current_minute': None,  # 当前自然分钟
                    'monthly_credits': stats_df[stats_df['api_key'] == key]['monthly_credits'].iloc[0] if not stats_df[stats_df['api_key'] == key].empty else 0
                }
                for key in self.api_keys
            }
        
        # 创建新的统计记录
        stats = {
            key: {
                'minute_calls': 0,
                'current_minute': None,
                'monthly_credits': 0
            }
            for key in self.api_keys
        }
        self._save_usage_stats(stats)
        return stats
        
    def _save_usage_stats(self, stats):
        """保存API使用统计"""
        current_utc = datetime.utcnow()
        current_month = current_utc.strftime('%Y%m')
        stats_file = self.stats_dir / f"api_usage_{current_month}.csv"
        
        stats_data = []
        for key, data in stats.items():
            stats_data.append({
                'api_key': key,
                'monthly_credits': data['monthly_credits']
            })
            
        pd.DataFrame(stats_data).to_csv(stats_file, index=False)

    def _get_available_key(self):
        """获取可用的API密钥"""
        current_utc = datetime.utcnow()
        current_month = current_utc.strftime('%Y%m')
        current_minute = current_utc.replace(second=0, microsecond=0)
        
        # 检查主密钥的月度使用情况
        main_key_stats = self.usage_stats[self.main_key]
        
        # 检查是否超过月度限制
        if main_key_stats['monthly_credits'] >= self.rate_limit['per_month']:
            # 主密钥达到月度限制
            send_wechat_message(f"主密钥已达到当月限制，切换至备用密钥")
            return self._get_backup_key()
        
        # 检查主密钥的分钟限制
        if main_key_stats['current_minute'] != current_minute:
            main_key_stats['current_minute'] = current_minute
            main_key_stats['minute_calls'] = 0
            
        if main_key_stats['minute_calls'] < self.rate_limit['per_minute']:
            return self.main_key
            
        # 主密钥达到分钟限制，使用备用密钥
        return self._get_backup_key()

    def _get_backup_key(self):
        """获取可用的备用密钥"""
        current_utc = datetime.utcnow()
        current_minute = current_utc.replace(second=0, microsecond=0)
        
        for key in self.backup_keys:
            key_stats = self.usage_stats[key]
            
            # 检查是否超过月度限制
            if key_stats['monthly_credits'] >= self.rate_limit['per_month']:
                continue  # 跳过已达到月度限制的密钥
            
            # 检查分钟限制
            if key_stats['current_minute'] != current_minute:
                key_stats['current_minute'] = current_minute
                key_stats['minute_calls'] = 0
                
            if key_stats['minute_calls'] < self.rate_limit['per_minute']:
                return key
                
        # 所有密钥都达到限制，等待下一分钟
        return None

    def _update_usage_stats(self, api_key, api_credit_consumed):
        """更新API使用统计"""
        stats = self.usage_stats[api_key]
        
        # 更新分钟调用次数
        stats['minute_calls'] += 1
        
        # 更新月度调用credit_count
        stats['monthly_credits'] += api_credit_consumed
        
        # 保存统计数据
        self._save_usage_stats(self.usage_stats)
        
    def _log_error(self, message, error):
        """记录错误详细信息"""
        error_details = traceback.format_exc()
        self.logger.error(f"{message}: {str(error)}\n{error_details}")
    
    @retry(max_tries=3)
    def _make_request(self, url, params):
        """执行API请求"""
        headers = {
            "X-CMC_PRO_API_KEY": self.api_key,
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        result = response.json()
            
        return result


    def call_api(self, endpoint, params):
        """调用API的主要方法"""
        # 检查月份是否已变更，如果变更则重新加载统计
        current_month = datetime.utcnow().strftime('%Y%m')
        if current_month != self.current_month:
            self.current_month = current_month
            self.usage_stats = self._load_usage_stats()  # 重新加载当月统计
        
        url = f"{self.base_url}{endpoint}"
        
        while True:
            api_key = self._get_available_key()
            if api_key:
                self.api_key = api_key
                result = self._make_request(url, params)
                
                # 更新API使用统计，使用响应中的实际credit_count
                api_credit_consumed = result.get('status', {}).get('credit_count', 1)
                self._update_usage_stats(self.api_key, api_credit_consumed)
                
                return result, api_credit_consumed
            
            # 所有密钥都达到限制，等待下一分钟
            current_utc = datetime.utcnow()
            next_minute = (current_utc + timedelta(minutes=1)).replace(second=0, microsecond=0)
            sleep_time = (next_minute - current_utc).total_seconds()
            self.logger.warning(f"所有API密钥已达到分钟限制，等待 {sleep_time:.2f} 秒后重试")
            time.sleep(sleep_time)

            if not api_key:
                send_wechat_message("⚠️ 密钥分钟容量达到上限")

    # ============= 具体API方法 =============
    def get_spot_pairs(self, network_slug, **kwargs):
        """
        获取代币的交易对列表
        """
        endpoint = '/dex/spot-pairs/latest'
        params = {
            "network_slug": network_slug,
            **kwargs
        }
        return self.call_api(endpoint, params)        

    def get_pair_quotes(self, network_slug, contract_addresses, **kwargs):
        """
        获取交易对报价信息
        """
        endpoint = '/dex/pairs/quotes/latest'
        params = {
            "network_slug": network_slug,
            "contract_address": ','.join(contract_addresses) if isinstance(contract_addresses, list) else contract_addresses,
            **kwargs
        }
        return self.call_api(endpoint, params)

    def get_pair_ohlcv(self, network_slug, contract_address, **kwargs):
        """
        获取交易对K线数据
        """
        endpoint = '/dex/pairs/ohlcv/historical'
        params = {
            "network_slug": network_slug,
            "contract_address": contract_address,
            **kwargs
        }
        return self.call_api(endpoint, params)
    
    # ============= K线获取 =============
    def fetch_klines_df(self, chain, contract_address, interval, time_end=None, time_start=None, limit=15, min_count=29):
        """
        chain: 链名称
        contract_address: 交易对合约地址
        interval: Default:"daily";"daily" "hourly" "1m" "5m" "15m" "30m" "4h" "8h" "12h" "weekly" "monthly"
        time_end: 获取K线数据的结束时间,填写None时,使用当前时间,
        time_start: 获取K线数据的开始时间,用来增量更新
        如果time_end和time_start都没有填写,用来获取全部K线
        min_count: 获取K线数据的最小数量,默认500条
        获取K线数据,K线数据常有缺失
        使用说明：
        1. 获取所有K线, 不需要填写time_end和time_start
        2. 更新K线, 需要填写time_start, 获取time_start到当前时间的K线
        """
        # 构建请求参数
        params = {
            "network_slug": chain,
            "contract_address": contract_address,
            "interval": interval,
            "time_period": interval,
            "count": limit,
            "convert_id": "2781",
            "skip_invalid": True,
        }       
            
        if time_start:
            start_time = pd.to_datetime(time_start).strftime('%Y-%m-%dT%H:%M:%S.000Z')
            params["time_start"] = start_time
        
        all_klines = pd.DataFrame()
        total_credit_count = 0
        while True:
            try:
                result, api_credit_consumed = self.get_pair_ohlcv(**params)
                total_credit_count += api_credit_consumed
                if not result.get('data') or len(result['data']) == 0:
                    break
                
                data = result['data'][0]
                
                # 处理基础信息
                base_info = {
                    "pair_name": data["name"],
                    "pair_address": data["contract_address"],
                    "symbol": data["base_asset_symbol"],
                    "address": data["base_asset_contract_address"],
                    "quote_coin_symbol": data["quote_asset_symbol"],
                    "chain": data["network_slug"],
                    "created_at": data["created_at"],
                }
                
                # 处理quotes数据
                processed_klines = []
                for quote_item in data["quotes"]:
                    quote_data = quote_item["quote"][0]
                    time_data = {
                        "candle_begin_time": quote_item["time_open"],
                        "open": quote_data["open"],
                        "high": quote_data["high"],
                        "low": quote_data["low"],
                        "close": quote_data["close"],
                        "volume": quote_data["volume"],
                    }
                    processed_klines.append({**base_info, **time_data})
                    
                # 根据请求类型更新参数
                if time_start:
                    if interval.endswith("m"):
                        time_start = pd.to_datetime(processed_klines[-1]["candle_begin_time"]) + timedelta(minutes=int(interval[:-1]))
                    elif interval.endswith("h"):
                        time_start = pd.to_datetime(processed_klines[-1]["candle_begin_time"]) + timedelta(hours=int(interval[:-1]))
                    params["time_start"] = time_start.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                else:
                    # 将时间减去interval，并确保格式为ISO 8601
                    if interval.endswith("m"):
                        time_end = pd.to_datetime(processed_klines[0]["candle_begin_time"]) - timedelta(minutes=int(interval[:-1]))
                    elif interval.endswith("h"):
                        time_end = pd.to_datetime(processed_klines[0]["candle_begin_time"]) - timedelta(hours=int(interval[:-1]))
                    params["time_end"] = time_end.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                
                all_klines = pd.concat([all_klines, pd.DataFrame(processed_klines)])
                all_klines.drop_duplicates(subset=['candle_begin_time'], inplace=True)
                # 不是增量更新时，如果K线数量达到min_count，则停止
                if not time_start:
                    if min_count and len(all_klines) >= min_count:
                        break

                
            except Exception as e:
                self._log_error(f"获取{contract_address}最新K线数据失败", e)
                return pd.DataFrame(), total_credit_count

        if all_klines.empty:
            return pd.DataFrame(), total_credit_count
        
        all_klines['candle_begin_time'] = pd.to_datetime(all_klines['candle_begin_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        all_klines['created_at'] = pd.to_datetime(all_klines['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # K线整理
        all_klines = all_klines.drop_duplicates(subset=['candle_begin_time'])  # 去重
        all_klines = self._kline_uniform(all_klines)  # 标准化K线数据
        all_klines = all_klines.sort_values(by="candle_begin_time", ascending=True)  # 按时间排序
        
        return all_klines, total_credit_count

    def _kline_uniform(self, df):
        """K线数据标准化"""
        columns = [
            'candle_begin_time', 
            'open', 
            'high', 
            'low', 
            'close',
            'volume',
            "symbol",
            "address",
            "quote_coin_symbol",  
            "pair_name",
            "pair_address",   
            "chain",
            "created_at",
        ]
        df = df[columns]
        return df    
    
    # ============= 获取流动性最大的交易对 =============
    def get_pair_address_largest_liquidity(self, network_slug, token_address=None, **kwargs):
        """
        获取流动性最大的交易对合约地址
        直接获取排序最大的池子2个
        network_slug: 链名称，如 solana, bsc 等
        token_address: 币种合约地址，可选参数
        返回: (pair_address, pair_name, liquidity) 元组，如果未找到则返回 (None, None, 0)
        """
        params = {
            "network_slug": network_slug,
            "convert_id": "2781",  # 使用USDT作为计价货币
            "sort": "liquidity",
            "sort_dir": "desc",
            "limit": 2,            
            **kwargs
        }
        # 统计使用的credit_count
        total_credit_count = 0
        
        # 如果指定了币种合约地址，则添加到参数中
        if token_address:
            params["base_asset_contract_address"] = token_address
            
        # 获取交易对列表
        result, api_credit_consumed = self.get_spot_pairs(**params)
        total_credit_count += api_credit_consumed
        
        if not result or 'data' not in result or not result['data']:
            # 去掉base_asset_contract_address，使用quote_asset_contract_address
            if "base_asset_contract_address" in params:
                params.pop("base_asset_contract_address")
            params["quote_asset_contract_address"] = token_address
            result, api_credit_consumed = self.get_spot_pairs(**params)
            total_credit_count += api_credit_consumed
            
            # 如果仍然没有数据，则返回None
            if not result or 'data' not in result or not result['data']:
                return None, None, 0, total_credit_count
            
        # 获取所有交易对并按流动性排序（从高到低）
        pairs = result['data']
        sorted_pairs = sorted(pairs, key=lambda x: float(x['quote'][0]['liquidity']) if x.get('quote') and len(x['quote']) > 0 and x['quote'][0].get('liquidity') is not None else 0.0, reverse=True)
        
        if not sorted_pairs:
            return None, None, 0, total_credit_count
            
        # 获取流动性最大的交易对信息
        max_liquidity_pair = sorted_pairs[0]
        pair_address = max_liquidity_pair['contract_address']
        pair_name = max_liquidity_pair['name']
        liquidity = float(max_liquidity_pair['quote'][0]['liquidity'])
        
        return pair_address, pair_name, liquidity, total_credit_count


# 如果直接运行此文件，则执行测试
if __name__ == "__main__":
    # 测试函数
    try:
        # 初始化客户端
        from config import cmc_api_keys
        client = CMCClient(cmc_api_keys)
        
        print("=== 测试 CMC API 轻量客户端 ===")
        print("1. 测试获取交易对")
        
        # 测试币种地址
        ca = '8pGeXS65kYBvS37jf3imYWkdPwM8seeTxrxMSSpbpump'
        
        # 测试作为base_asset和quote_asset获取
        print("\n测试 get_pair_address_largest_liquidity 函数")
        pair_address, pair_name, liquidity = client.get_pair_address_largest_liquidity(
            network_slug="solana", 
            token_address=ca
        )
        
        if pair_address:
            print(f"✅ 成功获取交易对信息: {pair_name}, 地址: {pair_address}, 流动性: {liquidity}")
        else:
            print("❌ 交易对获取失败")
        
        # 测试获取K线数据
        print("\n测试获取K线数据")
        if pair_address:
            klines = client.fetch_klines_df(
                chain="solana",
                contract_address=pair_address,
                interval="5m",
                # time_start="2025-04-14 09:00:00"
            )
            
            if klines.empty:
                print("❌ 未获取到K线数据")
            else:
                print(f"✅ 成功获取 {len(klines)} 条K线数据")
                print("K线数据示例:")
                print(klines.head())
            
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        traceback.print_exc()
        exit() 