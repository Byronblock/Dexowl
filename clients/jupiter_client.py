#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Jupiter Ultra API客户端
使用JavaScript进行交易签名
支持Jupiter Ultra API v1
目前只支持使用SOL进行买卖
"""

import os
import json
import subprocess
import requests
import base58
from typing import Dict, Any

from utils.log_kit import logger, divider
from config import root_path, proxy
from utils.commons import retry

class JupiterClient:
    
    def __init__(self, public_key=None, private_key=None, node_path=None):
        """
        初始化Jupiter客户端
        
        private_key: Solana钱包私钥（base58格式，如果不提供，仅能获取报价）
        node_path: Node.js可执行文件路径（默认使用'node'命令）
        """
        self.api_base_url = "https://lite-api.jup.ag"
        
        # 设置JS脚本路径
        self.js_signer_path = str(root_path / "clients" / "jupiter_signer" / "jupiter_signer.js")
        
        # 设置钱包
        self.public_key = public_key
        self.private_key = private_key
        logger.info(f"已加载钱包: {public_key}")

        # 配置代理
        self.proxies = proxy
        
        # 设置Node.js路径
        self.node_path = node_path or 'node'
        
        # 创建session以复用连接
        self.session = requests.Session()
        
    @retry(max_tries=3, delay_seconds=1, backoff=2, exceptions=(requests.exceptions.RequestException,))
    def _make_get_request(self, url, params=None):
        """
        执行GET请求并添加重试机制
        """
        response = self.session.get(url, params=params, proxies=self.proxies, timeout=15)
        response.raise_for_status()
        return response.json()

    
    @retry(max_tries=3, delay_seconds=1, backoff=2, exceptions=(requests.exceptions.RequestException,))
    def _make_post_request(self, url, data=None, json=None):
        """
        执行POST请求并添加重试机制
        """
        response = self.session.post(url, data=data, json=json, proxies=self.proxies, timeout=15)
        response.raise_for_status()
        return response.json()

    # ====================交易相关函数======================
    def get_order(self, input_mint: str, output_mint: str, amount: str, slippage_bps: int = None,
                  public_key: str = None):
        """
        获取Jupiter交易订单

        input_mint: 本币地址, 比如SOL
        output_mint: 目标币地址, 各种meme币
        amount: 本币交易数量（以输入代币的最小单位计）
        slippage_bps: 滑点容忍度（基点，100 = 1%）
        public_key: 钱包地址，如果为None，则使用初始化时设置的钱包地址
            
        Returns: {
            "transaction": "base64编码的交易数据", 用于sign_transaction
            "requestId": "订单请求ID", 用于execute_order
        }
        """
        url = f"{self.api_base_url}/ultra/v1/order"
        
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": amount,
            "taker": public_key or self.public_key,
        }
        # 如果没有传入滑点，就让jupiter自己决定
        if slippage_bps:
            params["slippageBps"] = slippage_bps

        order = self._make_get_request(url, params)
        
        # 检查响应
        if "transaction" not in order:
            logger.warning("订单中没有交易数据，可能是API限制或钱包地址无效")
        else:
            if "inAmount" in order and "outAmount" in order:
                if input_mint == "So11111111111111111111111111111111111111112":
                    # 这是买入
                    sol_amount = int(order["inAmount"]) / 10**9  # SOL，小数点9位
                    symbol_amount = int(order["outAmount"]) / 10**6  # 小数点6位, meme是6位
                    price = sol_amount / symbol_amount if symbol_amount else 0
                    logger.ok(f"报价: {sol_amount} -> {symbol_amount}, 价格: {price:.8f} sol")
                else:
                    # 这是卖出
                    symbol_amount = int(order["inAmount"]) / 10**6  # 小数点6位, meme是6位
                    sol_amount = int(order["outAmount"]) / 10**9  # SOL，小数点9位
                    price = sol_amount / symbol_amount if symbol_amount else 0
                    logger.ok(f"报价: {symbol_amount} -> {sol_amount}, 价格: {price:.8f} sol")
        
        return order
    
    def sign_transaction(self, transaction_base64: str) -> Dict[str, Any]:
        """
        使用JavaScript签名交易
        transaction_base64: 通过get_order返回的base64编码的交易数据
        Returns: {
            "signedTransaction": "已签名交易", 用于execute_order
        }
        """
                
        if not self.private_key:
            return {"error": "未设置钱包私钥"}

        private_key_bytes = base58.b58decode(self.private_key)
        private_key_bs58 = base58.b58encode(private_key_bytes).decode('utf-8')
        
        # 调用JS签名脚本
        cmd = [self.node_path, self.js_signer_path, "sign", transaction_base64, private_key_bs58]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        
        # 解析输出
        if result.returncode != 0:
            logger.error(f"签名脚本执行失败: {result.stderr}")
            return {"error": f"签名脚本执行失败: {result.stderr}"}

        return json.loads(result.stdout)
   
    def execute_order(self, signed_transaction: str, request_id: str) -> Dict[str, Any]:
        """
        执行已签名的交易订单
        signed_transaction: 通过sign_transaction返回的已签名交易
        request_id: 通过get_order返回的订单请求ID
        Returns: 交易执行结果
        """
        url = f"{self.api_base_url}/ultra/v1/execute"
        payload = {
            "signedTransaction": signed_transaction,
            "requestId": request_id
        }
        
        result = self._make_post_request(url, json=payload)
        return result
    
    # ====================查询======================
    def get_balances(self, wallet_address=None):
        """
        获取钱包的代币余额列表
        wallet_address: 钱包地址（如不提供则使用当前加载的钱包）
        除了SOL,其余代币都是用hash值作为key
        Returns: {
            "SOL": {
                "amount": "708333230",
                "uiAmount": 0.70833323,
                "slot": 334724619,
                "isFrozen": false
            },
            "FnxFDtxWwYDDEeYhgQMTbimSJiR7rbKUtBAAad3E4dfq": {
                "amount": "243461942894",
                "uiAmount": 243461.942894,
                "slot": 334724619,
                "isFrozen": false
            },
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
                "amount": "25442597",
                "uiAmount": 25.442597,
                "slot": 334724619,
                "isFrozen": false
            }            
        }
        """
            
        # 构建API URL
        if not wallet_address:
            wallet_address = self.public_key
        
        url = f"{self.api_base_url}/ultra/v1/balances/{wallet_address}"
        
        balances = self._make_get_request(url)
        return balances

    def get_token_info(self, address: str) -> Dict[str, Any]:
        """
        获取代币信息
        """
        url = f"{self.api_base_url}/tokens/v1/{address}"
        
        token_info = self._make_get_request(url)
        return token_info
    
    def get_all_tradable_tokens(self):
        """
        获取所有可交易的代币
        """
        url = f"{self.api_base_url}/tokens/v1/mints/tradable"
        tokens = self._make_get_request(url)
        return tokens
    
    # ====================一键交易======================
    def swap(self, 
             input_mint: str, 
             output_mint: str, 
             amount: str, 
             slippage_bps: int = None,
             public_key: str = None) -> Dict[str, Any]:
        """
        封装好的交易函数（获取订单、签名、执行）
        input_mint: 输入代币的mint地址
        output_mint: 输出代币的mint地址
        amount: 交易金额（以输入代币的最小单位计）
        slippage_bps: 滑点容忍度（基点，100 = 1%）
        public_key: 钱包地址，如果为None，则使用初始化时设置的钱包地址
        return: {
            'signature': 交易签名
            'slot': 区块高度
            'inputAmountResult': 输入金额
            'outputAmountResult': 输出金额
        }
        """
        if not self.private_key or not self.public_key:
            return {"error": "未设置钱包私钥或公钥"}

        # 1. 获取订单
        order = self.get_order(input_mint, output_mint, amount, slippage_bps, public_key)
        
        if "transaction" not in order:
            return {"error": "订单中没有交易数据"}
        
        # 2. 签名交易
        sign_result = self.sign_transaction(order["transaction"])

        if "signedTransaction" not in sign_result:
            return {"error": "签名结果中没有已签名交易"}
        
        # 3. 执行交易
        execute_result = self.execute_order(
            sign_result["signedTransaction"],
            order["requestId"]
        )
        
        return execute_result


# 示例用法
if __name__ == "__main__":
    # Solana代币地址
    SOL_MINT = "So11111111111111111111111111111111111111112"  # SOL
    USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
    
    # 交易金额: 0.01 SOL (1 SOL = 10^9 lamports)
    AMOUNT = "1000000"
    
    # 从环境变量获取私钥
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.warning("未安装python-dotenv，将不会加载.env文件")
    
        
    # 创建客户端实例
    client = JupiterClient(
        public_key='3M1rU8cCZMbUioYMX4CVSVpDciTxkDHQif8Av2GtWu9f',
        private_key=os.getenv("account_1_private_key"),  # 如果不提供，只能获取报价
    )

    # 获取交易报价
    # order = client.get_order(SOL_MINT, USDC_MINT, AMOUNT, slippage_bps=500)
    # print(f"订单信息: {json.dumps(order, indent=2)}")
    balance = client.get_balances()
    usdc_balance = balance[USDC_MINT]['amount']
    usdc_ui_amount = balance[USDC_MINT]['uiAmount']
    print(f"USDC余额: {usdc_balance} {usdc_ui_amount}")

    result  = client.swap(USDC_MINT, SOL_MINT, usdc_balance, slippage_bps=500)
    print(f"交换结果: {result}")


    