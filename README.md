# market-live · 实时市场看板

> **国内直连**：https://homjanon.github.io/market-live/（GitHub Pages，中国内地直连）  
> **VPN**：https://market-live.homjanon.workers.dev（Cloudflare Worker，含手动刷新按钮）

实时展示 A 股、港股、美股、全球主要指数、大宗商品、汇率、估值水位，并自算**小旭恐惧指数（XXFI）** 与 **A 股冰点**参考指标。

---

## 架构 · 数据流

```
┌─────────────────────────────────────────────────┐
│           Cloudflare Python Worker              │
│  (Pyodide 运行时, Cron 每30分钟触发)             │
│                                                   │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐   │
│  │ 东方财富  │  │ 新浪财经  │  │ 乐咕乐股 legu  │   │
│  │ push2delay│  │ sina hf_  │  │ www.legulegu   │   │
│  │           │  │ sina kline│  │ .com           │   │
│  └────┬─────┘  └────┬─────┘  └──────┬────────┘   │
│       │             │               │            │
│       ▼             ▼               ▼            │
│  ┌─────────────────────────────────────────┐     │
│  │  9 路并发抓取 → build_snapshot()        │     │
│  │  · 指数/汇率/商品 · 广度/资金流/估值    │     │
│  │  · 自算 XXFI + 冰点                    │     │
│  └────────────────┬────────────────────────┘     │
│                   │                              │
│          ┌────────┴────────┐                     │
│          ▼                 ▼                      │
│  ┌─────────────┐  ┌───────────────┐              │
│  │ KV (workers │  │ GitHub API    │              │
│  │  .dev 读取)  │  │ PUT data.json │              │
│  └──────┬──────┘  └───────┬───────┘              │
└─────────┼─────────────────┼──────────────────────┘
          │                 │
          ▼                 ▼
┌─────────────────┐  ┌──────────────────────────┐
│ workers.dev     │  │ GitHub Pages              │
│ (需VPN)         │  │ github.io (国内直连)      │
│ 读 /api/data    │  │ 读 ./data.json (只读)     │
│ 含手动刷新按钮   │  │ 无手动刷新                │
└─────────────────┘  └──────────────────────────┘
```

**核心逻辑**：Worker 每30分钟采集9路数据源 → 计算 XXFI + 冰点 → 同时写入 KV（供 VPN 版读取）和推送到 GitHub Pages（供国内直连）。

---

## 触发规则

| 规则 | 值 |
|---|---|
| **Cron**（UTC） | `45,15 1-8 * * 1-5` |
| **Cron**（北京） | `9:45, 10:15, 10:45, …, 16:15`（每 30 分钟） |
| **首次触发** | 北京 **9:45** |
| **末次触发** | 北京 **16:15** |
| **交易日** | **A 股交易日**（通过 legu 页面统计日期自动校验，含调休） |
| **周末** | ❌ 跳过 |
| **节假日** | ❌ 跳过（春节/国庆/清明等） |

双重守卫：
- `in_trading_window()` — 北京时 9:30–16:15、周一~周五
- `is_legu_today()` — 检查 legu 页面统计日期是否等于当日

---

## 数据板块

### 📈 A 股（6 只）

| 指数 | 来源 | secid |
|---|---|---|
| 上证指数 | 东方财富 push2delay | `1.000001` |
| 深证成指 | 东方财富 push2delay | `0.399001` |
| 创业板指 | 东方财富 push2delay | `0.399006` |
| 沪深300 | 东方财富 push2delay | `1.000300` |
| 科创50 | 东方财富 push2delay | `1.000688` |
| **北证50** | 东方财富 push2delay | `0.899050` |

### 📊 港股（2 只）

| 指数 | 来源 | secid |
|---|---|---|
| 恒生指数 | 东方财富 push2delay | `100.HSI` |
| 恒生科技 | 东方财富 push2delay | `124.HSTECH` |

### 🇺🇸 美股（3 只）

| 指数 | 来源 | secid |
|---|---|---|
| 纳斯达克100 | 东方财富 push2delay | `100.NDX` |
| 标普500 | 东方财富 push2delay | `100.SPX` |
| 道琼斯 | 东方财富 push2delay | `100.DJIA` |

### 🌍 全球（6 只）

| 指数 | 来源 | secid |
|---|---|---|
| 日经225 | 东方财富 push2delay | `100.N225` |
| 韩国KOSPI | 东方财富 push2delay | `100.KS11` |
| 德国DAX | 东方财富 push2delay | `100.GDAXI` |
| **欧洲斯托克600** | 东方财富 push2delay | `100.SXXP` |
| **法国CAC40** | 东方财富 push2delay | `100.FCHI` |
| **英国富时100** | 东方财富 push2delay | `100.FTSE` |

### 🛢️ 大宗商品（3 只）

| 品种 | 来源 | symbol |
|---|---|---|
| WTI 原油 | 新浪 hf\_ | `hf_CL` |
| COMEX 黄金 | 新浪 hf\_ | `hf_GC` |
| 布伦特原油 | 新浪 hf\_ | `hf_OIL` |

### 💱 汇率（1 只）

| 品种 | 来源 | secid |
|---|---|---|
| 美元离岸人民币 | 东方财富 push2delay | `133.USDCNH` |

---

## 计算指标

### 小旭恐惧指数（XXFI）

恐惧 4 项 + 贪婪 5 项，加权合成恐惧指数（XXFI = 恐惧总分）。与 [xiaoxu-fear](https://github.com/homjanon/xiaoxu-fear) 完全同源计算。

**恐惧分量（值越高→越恐惧→反向看多）：**

| 分量 | 权重 | 公式 | 数据来源 |
|---|---|---|---|
| 回撤 drawdown | 0.30 | clamp(abs(dd)×500) | 上证指数 20 日 K + 当日 spot（新浪） |
| 广度 breadth | 0.25 | clamp((down/up-0.5)×100) | 乐咕乐股 legu（全市场涨跌家数） |
| 跌停比 limitdown | 0.20 | clamp((ld/lu)×50) | 乐咕乐股 legu（涨停/跌停家数） |
| 波动率 vol | 0.25 | clamp(vol_pct×100) | 上证指数 20 日波动率分位（新浪） |

**贪婪分量（值越高→越贪婪→反向看空）：**

| 分量 | 权重 | 公式 | 数据来源 |
|---|---|---|---|
| 动量 momentum | 0.25 | clamp(ret20×500) | 上证指数 20 日涨幅（新浪） |
| 涨停比 limitup | 0.15 | clamp((lu/ld)×50) | 乐咕乐股 legu |
| 散户追高 retailin | 0.20 | clamp(retail_net×200) | 东方财富 ulist（沪深两市资金流） |
| 超买 overbought | 0.20 | clamp(above×500) | 上证指数高于 20 日均线幅度 |
| 背离 divergence | 0.20 | clamp(50−div×200) | 主力−散户（东财资金流） |

**XXFI 信号阈值**：

| XXFI | 信号 | 含义 |
|---|---|---|
| ≥75 | **BUY** | 极度恐惧，分批低吸 |
| 60–74 | **ACCUMULATE** | 恐惧，逢低吸纳 |
| 40–59 | **HOLD** | 中性，按策略持有 |
| 25–39 | **REDUCE** | 偏贪婪，逢高减仓 |
| <25 | **SELL** | 极度贪婪，减仓避险 |

### A 股冰点（4 维度，全部满足 = 冰点）

| 维度 | 阈值 | 数据源 |
|---|---|---|
| D1 下跌广度 | 下跌 ≥ 4000 且 占比 ≥ 85% | legu 全市场涨跌家数 |
| D2 指数/ETF 跌幅 | 上证 ≤ -2.0% 且 创业板 ≤ -2.5% 且 ETF 跌 ≤ -2.5% 占比 ≥ 60% | 东财指数 + 45 只核心 ETF 白名单 |
| D3 跌停数量 | 跌停 ≥ 50 且 跌停/涨停 ≥ 3 | legu 涨停/跌停家数 |
| D4 放量恐慌 | 放量倍数（当日成交额/近 20 日均额）≥ 1.3 | 东财 ulist（当日额）+ KV 滚动缓存（20日均额，首次从腾讯newfqkline补齐） |

核心 ETF 白名单共 **45 只**（宽基 11 只 + 行业 34 只，含中证2000 微盘、保险/能源/交运/半导体等补充项）。

### 🇺🇸 美股恐慌指数

在 A 股冰点卡下方展示，包含三个指标：

| 指标 | 含义 | 数据源 | 解读区间 |
|---|---|---|---|
| **VIX** | 标普500 30天隐波 | Yahoo Finance | ≥40极度恐慌 / ≥30恐慌 / ≥25偏恐慌 / ≥20偏高 / ≥15正常 / ≥12偏低 / <12极低贪婪 |
| **VXN** | 纳斯达克100 30天隐波 | Yahoo Finance | 同上（纳指波动率通常高于标普） |
| **CNN Fear & Greed** | 7因子恐惧贪婪指数（0–100） | CNN dataviz API | ≤25极度恐慌 / ≤45恐慌 / ≤55中性 / ≤75贪婪 / >75极度贪婪 |

### 估值水位

| 指数 | 来源 |
|---|---|
| 标普500 | 蛋卷基金 index_eva |
| 创业板指 | 蛋卷基金 index_eva |
| 中证红利低波 | 蛋卷基金 index_eva |
| 恒生科技 | 蛋卷基金 index_eva |
| 沪深300 | 蛋卷基金 index_eva |

---

## 文件结构

```
market-live/
├── worker.py              # Cloudflare Python Worker 主逻辑
│   ├─ 采集 9 路数据源     # (HTTP 并发, asyncio.gather)
│   ├─ compute_xxfi()      # 小旭恐惧指数计算
│   ├─ compute_bingdian()  # A 股冰点判定
│   ├─ publish_to_github() # 推 data.json 到 GitHub
│   └─ class Default       # Worker 入口 (fetch + scheduled)
├── wrangler.toml          # Cloudflare 部署配置
│   ├─ KV 绑定             # 快照存储
│   ├─ ASSETS 绑定         # 静态面板
│   └─ Cron 触发           # 45,15 1-8 * * 1-5
├── public/
│   └── index.html         # Worker 版前端（VPN 可用，含手动刷新按钮）
├── docs/                  # GitHub Pages 投递目录
│   ├── index.html         # GitHub Pages 版前端（国内直连，只读快照）
│   └── data.json          # 实时快照（Worker 每 30 分钟自动推送）
├── pyproject.toml         # Python 项目配置
├── pylock.toml            # uv 依赖锁
├── README.md              # 本文件
└── DEPLOY_STATUS.md       # 部署状态与演进记录
```

---

## 技术栈

| 层 | 技术 |
|---|---|
| **计算引擎** | Cloudflare Python Worker（Pyodide 运行时，`python_workers` 兼容标志） |
| **数据存储** | Cloudflare Workers KV（快照缓存） |
| **静态面板** | Cloudflare Workers Assets（原生 HTML/JS，无框架） |
| **国内投递** | GitHub Pages（零服务器费，github.io 中国内直连） |
| **数据推送** | Worker → GitHub Contents API → `docs/data.json` |
| **凭证管理** | GitHub PAT 以 Cloudflare Secret 形式加密存储，不进代码 |
| **Cron** | Cloudflare Triggers（每 30 分钟） |
| **部署工具** | `pywrangler`（workers-py ≥1.90）→ `wrangler deploy` |

---

## 部署指南

```bash
# 1. 安装依赖
uv sync

# 2. 设置 GitHub PAT（用于数据推送）
echo "你的github_token" | CLOUDFLARE_API_TOKEN="你的cf_token" CLOUDFLARE_ACCOUNT_ID="你的cf_account_id" uv run pywrangler secret put GITHUB_TOKEN

# 3. 部署 Worker
CLOUDFLARE_API_TOKEN="..." CLOUDFLARE_ACCOUNT_ID="..." uv run pywrangler deploy

# 4. GitHub Pages 设置（仅首次）
# 仓库 Settings → Pages → Source: Deploy from branch → main → /docs → Save
```

---

## 与 xiaoxu-fear 的关系

本项目的 XXFI 计算与 [xiaoxu-fear](https://github.com/homjanon/xiaoxu-fear) 完全同源（公式、权重、阈值、文本逐行一致），差异仅在于：

- xiaoxu-fear 使用 akshare 取数（本地/GitHub Actions）
- market-live 使用直连 HTTP API 取数（Cloudflare Worker 无 akshare）
- xiaoxu-fear 输出为 GitHub Pages 静态报告
- market-live 为实时滚动看板（每 30 分钟刷新）

---

## 数据来源

| 来源 | 用途 | 协议 |
|---|---|---|
| **东方财富** push2delay | A/港/美/全球指数、汇率、资金流、ETF、K-line（成交额） | 公开 HTTP API |
| **新浪财经** sina hf\_ | 全球大宗商品实时行情 | 公开 HTTP API |
| **新浪财经** K-line API | 上证指数日K（回撤/波动率/动量） | 公开 HTTP API |
| **乐咕乐股** legulegu.com | 全市场涨跌/涨停/跌停家数（盘面广度） | 公开页面解析 |
| **蛋卷基金** danjuanfunds.com | 指数估值 PE/PB/分位/股息率 | 公开 HTTP API |
| **GitHub** api.github.com | 投递 data.json 到 Pages 仓库 | OAuth PAT |

> **免责声明**：所有数据均来自公开网络接口，仅供研究参考，不构成投资建议。数据实时性受限于各源更新频率。
