# Dex_owl 项目文档
## 使用准备
* coinmarketcap申请api
* 钱包私钥
* 企业微信bot

## 1. 项目概述

Dex_owl 是一个用于去中心化交易所（DEX）的自动化策略交易框架。它旨在通过模块化的设计，实现数据获取、信号生成、仓位管理和交易执行等功能，帮助用户在不同的区块链网络上执行交易策略。项目强调代码的简洁性和逻辑的清晰性，并优先实现核心功能。

## 2. 核心模块

### 2.1 数据中心 (Talons)

数据中心负责从外部API获取和处理市场数据，主要包括K线数据和币池数据。

#### 2.1.1 数据中心启动程序 (`talons_startup.py`)

*   **初始化**: 根据配置文件 `config.py` 遍历并设置不同账户。
*   **数据加载**: 根据需求加载币池和K线相关内容，初始化数据环境。
*   **定时任务**:
    *   定期启动 `pools_generator.py` 以更新币池数据。
    *   定期启动 `klines_fetcher.py` 以获取最新的K线数据。
    *   `pools_generator` 和 `klines_fetcher` 的运行周期是相互独立的。
*   **时间控制**: 使用 `sleep_until_run_time()` 和 `next_run_time()` (位于 `utils/`) 控制任务的精确执行时间。

#### 2.1.2 K线获取 (`talons/klines_fetcher.py`)

*   **执行时机**: 每个整点 `interval` (例如：5分钟，如 21:05, 21:10, 21:15) 自动执行K线获取任务。
*   **数据源**:
    *   遍历不同的区块链网络。
    *   针对每条链，合并 `active_pool.csv` 和 `active_position.csv` 中的代币信息。
    *   提取并合并 "chain"、"address"、"symbol"、"pair_address" 四个关键字段，形成待处理的代币列表 (DataFrame)。
*   **Pair Address 处理**:
    *   检查代币列表中每个代币的 `pair_address` 是否存在。
    *   若 `pair_address` 缺失，则调用 `clients` 模块中的 `get_pair_address_largest_liquidity()` 方法获取。
        *   `get_pair_address_largest_liquidity()` 方法会尝试使用返回结果中的 "base_asset_contract_address" 和 "quote_asset_contract_address" 作为参数分别获取 `pair_address`，以确保能找到目标代币对应的交易对。
    *   获取到的 `pair_address` 将更新回对应代币在 `active_pool.csv` 中的记录 (`active_position.csv` 中的数据源自 `active_pool.csv`，因此会自动同步)。
*   **K线获取**:
    *   为代币列表中的每个代币获取K线数据。此过程支持单线程（用于调试）或多线程执行。
    *   直接获取所有历史K线数据。K线获取客户端 (`clients/CLIENT.py` 中的 `fetch_klines_df()`) 内置了最大K线数量 (`max_count`) 的限制。
*   **数据保存**: 获取到的K线数据将保存在 `data/CHAIN_NAME/klines/SYMBOL.parquet`。
*   **完成标记**: K线更新完成后，会记录一个完成标志 (flag)，用于通知其他模块数据已准备就绪。系统会定期清理旧的标志，仅保留最新的100条记录。

#### 2.1.3 币池获取 (`talons/pools_generator.py`)

*   **账户遍历**: 遍历 `config.py` 中配置的账户，确定需要更新币池的范围。
*   **数据更新与保存**:
    *   **活跃池子**: 按照预设的更新频率，通过 `gmgn_client.py` (或类似客户端) 获取最新的活跃币池数据 (`active_pool`)。
        *   更新后的数据会放入一个内部队列 (`pool_queue`)。
        *   同时，最新的活跃池子数据会保存到 `data/CHAIN_NAME/ACCOUNT_NAME/active_pool.pkl` 和 `data/CHAIN_NAME/ACCOUNT_NAME/active_pool.csv`。
    *   **历史池子**: 监听 `pool_queue`，将新获取的池子数据与内存中当日的历史池子记录进行对比。如果发现新增的池子，则更新当日的 `data/CHAIN_NAME/ACCOUNT_NAME/history_pools/YYYY-MM-DD.csv` 文件。

### 2.2 仓位管理 (Hunter)

仓位管理模块负责根据交易信号执行具体的开平仓操作，并进行风险控制。此模块设计为可使用多核并行处理，以提升信号计算和交易执行的效率。

#### 2.2.1 信号管理 (`hunter/position.py`)

*   **处理活跃仓位 (卖出决策)**:
    *   读取 `data_feed/ACCOUNT_NAME/active_position.csv` 中的现有持仓代币。
    *   调用 `signals/` 目录下指定的因子计算模块 (例如 `sma.py`) 中的 `calculate_signal()` 函数，对每个持仓代币计算最新的交易信号。
        *   `calculate_signal()` 函数接收代币的K线数据 (DataFrame) 作为输入，输出带有信号的DataFrame。
        *   判断信号DataFrame最后一行的 `candle_begin_time` (转换为UTC+8时区后) 是否大于等于当前运行周期的上一个 `interval` 点。若通过，则该信号有效。
        *   信号值为 `0` 表示平仓，信号值为 `1` 表示继续持有。
    *   将计算出的信号传递给 `risk_manager.py` 进行止盈止损判断。
    *   综合因子信号和风控信号，生成卖出订单列表。
*   **处理活跃池子 (买入决策)**:
    *   读取 `data/CHAIN_NAME/ACCOUNT_NAME/active_pool.csv` 中的代币，并排除掉已存在于 `active_position.csv` 中的代币，避免重复开仓。
    *   同样调用 `signals/` 目录下的因子计算模块，获取交易信号。
    *   信号值为 `1` 表示开仓，信号值为 `0` 表示不开仓。
    *   生成买入订单列表。

#### 2.2.2 止盈止损 (`hunter/risk_manager.py`)

*   **止盈**:
    *   当价格达到 `entry_price * 2` 时，平掉一半仓位。
    *   在 `active_position.csv` 中标记该仓位的 `status` 为 "take_profit"，并设置 `take_profit=True`。
    *   更新 `pnl` (盈利) = 止盈数量 * 止盈价格。
    *   更新 `active_position.csv`。
*   **止损**:
    *   当价格达到 `entry_price * 0.5` 时，全部平仓。
    *   在 `active_position.csv` 中标记该仓位的 `status` 为 "stop_loss"。
    *   更新 `active_position.csv` 和 `data_feed/ACCOUNT_NAME/history_positions/YYYY-MM-DD.csv`。

#### 2.2.3 仓位分配 (`hunter/allocation.py`)

*   决定新开仓位的具体金额。
*   限制账户的最大同时持仓数量。
*   （当前版本）只按照固定金额开仓 (例如，每次开仓0.1个SOL)。

#### 2.2.4 交易模块 (`hunter/trade.py`)

*   封装了用于在 Solana 网络上交易代币的 `jupiter_client.py`。
*   **注意**: `jupiter_client.py` 可能依赖位于 `jupiter_signer/` 目录下的 JavaScript 脚本。需确保这些脚本能被正确加载，否则应有提示机制。
*   目前暂无 BSC网络的交易客户端。

### 2.3 主程序 (`main.py`)

主程序是整个策略框架的入口和调度中心。

*   **初始化**:
    *   为每个账户初始化 `active_position.csv` 文件（若不存在则创建，并写入表头）。
    *   为每个账户创建 `history_positions/` 文件夹（若不存在）。
*   **运行周期**: 与K线获取 (`klines_fetcher.py`) 的运行间隔保持一致。
*   **确定可交易链**: 根据 `config.py` 文件中的 `trade_config` 信息，判断哪些区块链网络当前处于可交易状态 (`status` 为 `True`)。
*   **账户与信号执行**:
    *   遍历所有配置的账户。
    *   根据账户所属的链，等待该链对应K线数据的当前周期更新完成标志 (flag)。
    *   一旦标志确认，开始执行该账户的交易信号逻辑（调用 `hunter/position.py` 等模块内的函数）。
*   **核心执行步骤**:
    1.  **Step 1: 处理活跃仓位，获取卖出订单**:
        *   读取 `active_position.csv`。
        *   计算信号 (结合因子和风控)，确定是否平仓或部分平仓 (止盈)。
        *   生成卖出订单列表 (包含时间、价格、数量)。
    2.  **Step 2: 处理活跃池子，获取买入订单**:
        *   读取 `active_pool.csv`，排除已持仓代币。
        *   计算信号，确定是否开仓。
        *   生成买入订单列表 (包含时间、价格)。
    3.  **Step 3: 合并订单，进行资金分配**:
        *   整合 Step 1 和 Step 2 生成的订单，遵循“先卖后买”的原则。
        *   检查是否超过仓位上限（考虑因止损/清仓而释放的仓位）。
        *   为买入订单分配具体的交易金额/数量。
        *   返回最终待执行的订单列表。
    4.  **Step 4: 执行下单**:
        *   调用 `hunter/trade.py` 中的交易函数，按照目标DEX（如 Jupiter）的接口要求执行订单。
    5.  **Step 5: 更新当前仓位和历史仓位**:
        *   根据交易执行的结果，更新 `active_position.csv`。
        *   将已平仓的仓位记录到 `data_feed/ACCOUNT_NAME/history_positions/YYYY-MM-DD.csv`。

### 2.4 择时信号 (`signals/`)

此目录存放各种择时策略的因子计算脚本。

*   **输入**: 每个因子脚本接收特定代币的K线数据 (DataFrame)。
*   **输出**: 返回一个新的DataFrame，其中包含计算出的交易信号。
*   **信号定义**:
    *   `1`: 代表开仓 (做多)。
    *   `-1`: 代表平仓。
*   **策略类型**: 目前主要针对现货交易，仅支持做多。

## 3. 数据结构

项目的数据主要存储在 `data/` 目录下，并遵循一定的层级结构和格式规范。

*   **一级目录**: `data/`
*   **二级目录**: 以区块链名称命名，例如 `data/solana/`、`data/bsc/`。
*   **三级目录 (K线)**: `data/CHAIN_NAME/klines/SYMBOL.parquet` (例如: `data/solana/klines/SOL_USDC.parquet`)。
*   **三级目录 (账户相关)**: `data/CHAIN_NAME/ACCOUNT_NAME/`
    *   `active_pool.csv`: 当前关注的活跃币池。
    *   `active_pool.pkl`: `active_pool.csv` 的 pickle 版本，可能用于更快的读写。
    *   `history_pools/YYYY-MM-DD.csv`: 每日历史币池记录。
*   **账户交易数据 (`data_feed/`)**:
    *   `data_feed/ACCOUNT_NAME/active_position.csv`: 当前持有的活跃仓位。
    *   `data_feed/ACCOUNT_NAME/history_positions/YYYY-MM-DD.csv`: 每日历史平仓记录。

**数据格式参考**: 请参照 `reference/data_format/` 目录下的文件，以了解各数据文件的具体字段和格式要求。

## 4. 开发注意事项

*   **禁止使用类 (Class)**: 整个项目（除必要的第三方库外）应避免使用面向对象的类定义，以函数式编程为主，保持代码简洁。
*   **模块化**: 各功能模块应尽可能独立，降低耦合度。
*   **配置文件**: `config.py` 用于管理账户信息、API密钥、交易参数等。
*   **工具库**: `utils/` 目录下提供了一些通用工具函数，例如时间处理、日志记录、企业微信通知等。
*   **客户端**: `clients/` 目录下封装了与外部API（如CMC, GMGN, Jupiter）交互的客户端。
*   **依赖管理**: （如果后续添加）应有明确的依赖管理方式，如 `requirements.txt`。

## 5. 目录结构概览

Dex_owl/
├── talons/ # 数据中心模块
│ ├── init.py
│ ├── klines_fetcher.py # K线数据获取与处理
│ └── pools_generator.py # 币池数据获取与处理
├── hunter/ # 仓位管理与交易执行模块
│ ├── init.py
│ ├── position.py # 信号整合与订单生成
│ ├── risk_manager.py # 止盈止损管理
│ ├── allocation.py # 仓位资金分配
│ └── trade.py # 交易执行
├── signals/ # 择时信号因子目录
│ ├── init.py
│ └── sma.py # 示例：简单移动平均线因子
├── clients/ # 外部API客户端
│ ├── init.py
│ ├── cmc_client_lite.py
│ ├── gmgn_client.py
│ └── jupiter_client.py
├── utils/ # 通用工具函数
│ ├── init.py
│ ├── time_utils.py
│ └── logger.py
├── data_feed/ # 账户相关的交易数据输入 (由用户或其它系统提供)
│ └── ACCOUNT_NAME/
│ ├── active_position.csv
│ └── history_positions/
│ └── YYYY-MM-DD.csv
├── data/ # 项目运行时产生的数据
│ └── CHAIN_NAME/
│ ├── klines/
│ │ └── SYMBOL.parquet
│ └── ACCOUNT_NAME/
│ ├── active_pool.csv
│ ├── active_pool.pkl
│ └── history_pools/
│ └── YYYY-MM-DD.csv
├── jupiter_signer/ # Jupiter交易签名相关脚本 (若有)
├── reference/ # 参考资料
│ └── data_format/ # 数据格式定义参考
├── config.py # 项目配置文件
├── talons_startup.py # 数据中心启动脚本
├── main.py # 主程序入口
└── README.md # 项目说明文档