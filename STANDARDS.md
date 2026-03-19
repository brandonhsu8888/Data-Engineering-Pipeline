# QF5214 项目实施规范

本文档定义数据工程流水线开发、测试、部署的标准和最佳实践。

---

## 一、环境规范

### 1.1 Conda 环境

| 环境名 | 用途 |
|--------|------|
| `qf5214_project` | 主开发环境（需配置） |
| `data_analysis` | 临时验证环境（已有 pandas, yfinance） |

**激活方式**：
```bash
conda activate qf5214_project
```

### 1.2 依赖管理

新增依赖时，必须同步更新文档：

```bash
# 安装新包
conda install -n qf5214_project <package>

# 导出环境
conda env export -n qf5214_project > environment.yml
```

---

## 二、代码规范

### 2.1 Python 代码风格

- 遵循 **PEP 8** 风格指南
- 使用 `black` 格式化代码
- 使用 `ruff` 进行 linting

```bash
# 格式化
black .

# 检查
ruff check .
```

### 2.2 模块命名

| 模块类型 | 路径 | 示例 |
|----------|------|------|
| 数据访问层 | `pipeline/data_provider.py` | `SPXDataProvider` |
| 摄取引擎 | `pipeline/ingestion_engine.py` | `IngestionEngine` |
| ELT 管线 | `pipeline/elt_pipeline.py` | `ELTPipeline` |
| 模拟器 | `pipeline/simulators/*.py` | `ComprehensiveSimulator` |
| 工具函数 | `pipeline/utils.py` | `calculate_returns()` |
| 配置 | `pipeline/config.py` | `DATA_DIR`, `DB_PATH` |
| 仪表盘 | `pipeline/dashboard.py` | Streamlit app |

### 2.3 函数命名

| 操作 | 命名 | 示例 |
|------|------|------|
| 获取数据 | `get_<resource>` | `get_price()`, `get_fundamentals()` |
| 批量写入 | `ingest_<resource>` | `ingest_price_batch()` |
| 查询 | `query_<view>` | `query_earnings_surprise()` |
| 转换 | `transform_<resource>` | `unpivot_financials()` |

### 2.4 数据类型

- **日期格式**：统一使用 `YYYY-MM-DD` 字符串
- **时间戳**：统一使用 `YYYY-MM-DD HH:MM:SS`
- **货币**：统一使用 `DECIMAL(18, 6)`
- **音量**：统一使用 `BIGINT`

---

## 三、数据规范

### 3.1 Landing Zone 文件命名

| 数据类型 | 格式 | 示例 |
|----------|------|------|
| 价格 | `landing_zone/prices/price_YYYY-MM-DD.csv` | `price_2024-01-15.csv` |
| 财务 | `landing_zone/fundamentals/YYYY-MM-DD/TICKER_type_freq.csv` | `2024-01-31/AAPL_income_quarterly.csv` |
| Transcript PDF | `landing_zone/transcripts/TICKER_YYYY-MM-DD.pdf` | `AAPL_2024-02-01.pdf` |
| Silver Parquet | `silver/price/date=YYYY-MM-DD/data.parquet` | `date=2024-01-15/data.parquet` |

### 3.2 DuckDB 表命名

| 层级 | 表名前缀 | 示例 |
|------|----------|------|
| Bronze | `raw_` | `raw_price_stream` |
| Silver | `silver_` | `silver_price` |
| Gold | `v_` (视图) | `v_earnings_surprise` |

### 3.3 Schema 变更

所有表结构变更必须：
1. 创建 migration 脚本 (`migrations/001_add_xxx.sql`)
2. 在 migration 记录表中登记
3. 避免直接 ALTER 生产表

---

## 四、API 设计规范

### 4.1 DataProvider 接口约定

```python
class SPXDataProvider:
    """所有数据访问必须通过此类"""

    def get_price(self, ticker: str, date: str) -> pd.DataFrame:
        """
        Args:
            ticker: 股票代码 (如 'AAPL')
            date: 日期 'YYYY-MM-DD'
        Returns:
            DataFrame with columns: Date, Ticker, Open, High, Low, Close, Adj Close, Volume
            有数据返回 DataFrame，无数据返回空 DataFrame
        Raises:
            ValueError: if ticker not found
        """
        ...

    def get_fundamentals(self, ticker: str, freq: str = "quarterly") -> dict:
        """
        Returns:
            dict with keys: 'income', 'balance', 'cashflow', 'profile'
            有数据返回 dict，无数据返回空 dict
        """
        ...

    def get_transcript(self, ticker: str, date: str) -> bytes:
        """Returns raw PDF bytes. 有数据返回 bytes，无数据抛出 FileNotFoundError"""
        ...

    def list_transcripts(self, ticker: str = None, year: int = None) -> list:
        """
        Filter transcripts by ticker and/or year.
        Returns: [{'ticker': 'AAPL', 'date': '2024-02-01'}, ...]
        """
        ...

    def get_trading_dates(self, start_date: str, end_date: str) -> list:
        """返回指定范围内的所有交易日"""
        ...

    def get_ticker_list(self) -> list:
        """返回所有可用 ticker"""
        ...
```

### 4.2 错误处理

| 错误类型 | 处理方式 |
|----------|----------|
| 数据不存在 | `raise FileNotFoundError` 或返回空 DataFrame |
| 数据损坏 | `raise ValueError` 并记录日志 |
| 格式不匹配 | `raise DataIntegrityError` |

---

## 五、测试规范

### 5.1 单元测试

```python
# tests/test_data_provider.py
import pytest
from pipeline.data_provider import SPXDataProvider

def test_get_price_returns_correct_columns():
    provider = SPXDataProvider()
    df = provider.get_price('AAPL', '2024-01-15')
    assert list(df.columns) == ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    assert len(df) > 0

def test_get_price_invalid_ticker_raises():
    provider = SPXDataProvider()
    with pytest.raises(ValueError):
        provider.get_price('INVALID_TICKER', '2024-01-15')

def test_get_price_no_data_returns_empty():
    provider = SPXDataProvider()
    df = provider.get_price('AAPL', '2024-01-16')  # 周末无数据
    assert len(df) == 0
```

### 5.2 测试数据

- 使用 `tests/fixtures/` 存放小型测试数据集
- 不在测试中使用完整 20 年数据（性能问题）
- 每个接口必须有至少一个正向测试和一个异常测试

---

## 六、安全规范

### 6.1 禁止事项

- **禁止**将 API keys 或 tokens 提交到代码库
- **禁止**在日志中打印敏感数据（密码、token）
- **禁止**从非 HTTPS 源下载数据

### 6.2 敏感信息

如需配置 API key，使用环境变量：

```python
import os
API_KEY = os.environ.get('YFINANCE_API_KEY', '')  # 不硬编码
```

---

## 七、日志规范

### 7.1 日志级别

| 级别 | 用途 | 示例 |
|------|------|------|
| DEBUG | 开发调试 | `logger.debug(f"Loaded {len(df)} rows")` |
| INFO | 正常流程 | `logger.info("Ingestion completed")` |
| WARNING | 异常但可处理 | `logger.warning(f"Missing data for {ticker}")` |
| ERROR | 错误需调查 | `logger.error(f"Failed to connect to DB")` |

### 7.2 日志格式

```
%(asctime)s - %(name)s - %(levelname)s - %(message)s
2024-01-15 10:30:00 - ingestion_engine - INFO - Ingested 500 price records
```

---

## 八、Git 提交规范

### 8.1 提交信息格式

```
<type>: <short summary>

<body (optional)>

<footer (optional)>
```

Type 类型：
- `feat`: 新功能
- `fix`: Bug 修复
- `docs`: 文档更新
- `refactor`: 重构（无功能变化）
- `test`: 测试相关
- `chore`: 构建/工具变更

### 8.2 示例

```
feat: add SPXDataProvider.get_transcript method

- Returns raw PDF bytes for given ticker and date
- Raises FileNotFoundError if PDF not found
- Added unit tests for valid and invalid inputs

Closes #12
```

---

## 九、性能规范

### 9.1 数据读取

- **禁止**在循环中读取大文件（使用向量化操作）
- **禁止**每日扫描所有文件头（预建索引）
- 价格数据超过 100MB 时使用 chunked reading

### 9.2 数据库写入

- 批量写入 > 单条写入（使用 `executemany` 或 DataFrame `to_sql(if_exists='append')`）
- 大数据导入前创建索引
- 启用 WAL 模式提升并发写入性能

```python
con.execute("PRAGMA synchronous=NORMAL")  # 提升写入性能
con.execute("PRAGMA journal_mode=WAL")    # 并发读写
```

---

## 十、部署规范

### 10.1 开发流程

```
1. 从 main 创建 feature 分支
2. 开发 + 单元测试
3. 提交 PR + Code Review
4. 合并到 main
5. 部署到生产（如有）
```

### 10.2 上线检查清单

- [ ] 所有单元测试通过
- [ ] 新的数据接口有文档说明
- [ ] 环境变更已更新 `environment.yml`
- [ ] 无硬编码的敏感信息
- [ ] 日志级别设置正确（生产用 INFO）

---

## 十一、文档维护

### 11.1 必需文档

| 文档 | 更新时机 |
|------|----------|
| `CLAUDE.md` | 新增组件、架构变更 |
| `IMPLEMENTATION_PLAN.md` | 里程碑完成、计划调整 |
| `STANDARDS.md` | 规范变更 |
| 代码 docstring | 函数签名变更 |

### 11.2 Docstring 格式

```python
def get_price(self, ticker: str, date: str) -> pd.DataFrame:
    """
    Get OHLCV price data for a ticker on a specific date.

    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL')
        date: Date in 'YYYY-MM-DD' format

    Returns:
        DataFrame with columns: Date, Ticker, Open, High, Low, Close, Adj Close, Volume
        Returns empty DataFrame if no data for that date (e.g., weekend/holiday)

    Raises:
        ValueError: If ticker is not found

    Example:
        >>> provider = SPXDataProvider()
        >>> df = provider.get_price('AAPL', '2024-01-15')
        >>> print(df.head())
    """
    ...
```

---

本文档随项目迭代更新。如有疑问，请先查看 `IMPLEMENTATION_PLAN.md`。
