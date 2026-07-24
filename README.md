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
│  │  10 路并发抓取 → build_snapshot()        │     │
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

**核心逻辑**：Worker 每30分钟采集10路数据源 → 计算 XXFI + 冰点 → 同时写入 KV（供 VPN 版读取）和推送到 GitHub Pages（供国内直连）。

---

## 触发规则

| 规则 | 值 |
|---|---|
| **Cron**（UTC） | `45 1` / `15,45 2-7` / `15 8` `* * mon-fri`（3 段，共 14 次/交易日） |
| **Cron**（北京） | `9:45, 10:15, 10:45, …, 16:15`（每 30 分钟） |
| **首次触发** | 北京 **9:45** |
| **末次触发** | 北京 **16:15** |
| **交易日** | Cron 已限定 `mon-fri`，周末/节假日数据源不返回新数据时保留最近交易日快照 |
| **周末** | Cron 不触发 |
| **节假日** | Cron 仍触发但数据源无新数据，快照不变 |

> **设计决策（2026-07-24）**：移除了 `in_trading_window()` 和 `is_tx_today()` 双重守卫。Cron 自身的 `mon-fri` + 时间窗口已足够精确；节假日数据源不返回新行情，保留周五收盘快照可接受，不再需要 legu 页面日期比对。

### Cron 触发链路

`scheduled()` 入口 → **HTTP 自调用 `/api/cron_run`**（fetch 上下文拥有完整 `env`，含 KV + Secret bindings）→ `_cron_run()` → `refresh_and_store()` → 写 KV + 推 GitHub Pages。

Python Workers beta 中 `scheduled()` 的 `self.env` 可能为 None，故 **HTTP 自调用是唯一可靠路径**。兜底：若自调用失败，退而尝试 `self.env`（仅在非 None 时有效）。

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
| 回撤 drawdown | 0.30 | clamp(abs(dd)×500) | 上证指数 20 日 K + 当日 spot（新浪，兜底腾讯） |
| 广度 breadth | 0.25 | clamp((down/up-0.5)×100) | 乐咕乐股 legu（全市场涨跌家数） |
| 跌停比 limitdown | 0.20 | clamp((ld/lu)×50) | 乐咕乐股 legu（涨停/跌停家数） |
| 波动率 vol | 0.25 | clamp(vol_pct×100) | 上证指数 20 日波动率分位（新浪，兜底腾讯） |

**贪婪分量（值越高→越贪婪→反向看空）：**

| 分量 | 权重 | 公式 | 数据来源 |
|---|---|---|---|
| 动量 momentum | 0.25 | clamp(ret20×500) | 上证指数 20 日涨幅（新浪，兜底腾讯） |
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
| D2 指数/ETF 跌幅 | 上证 ≤ -2.0% 且 创业板 ≤ -2.5% 且 ETF 跌 ≥ 60% | 东财指数 + 45 只核心 ETF 白名单 |
| D3 跌停数量 | 跌停 ≥ 50 且 跌停/涨停 ≥ 3 | legu 涨停/跌停家数 |
| D4 放量恐慌 | 放量倍数（当日成交额/近 20 日均额）≥ 1.3 | 东财 K-line + KV 滚动缓存（腾讯当日额兜底） |

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
│   ├─ 采集 10 路数据源    # (HTTP 并发, asyncio.gather)
│   ├─ compute_xxfi()      # 小旭恐惧指数计算
│   ├─ compute_bingdian()  # A 股冰点判定
│   ├─ publish_to_github() # 推 data.json 到 GitHub Pages
│   ├─ refresh_and_store() # Cron 核心：抓取+计算+写KV+推GitHub
│   └─ class Default       # Worker 入口 (fetch + scheduled)
├── public/
│   └── index.html         # Worker 版前端（VPN 可用，含手动刷新按钮）
├── docs/                  # GitHub Pages 投递目录
│   ├── index.html         # GitHub Pages 版前端（国内直连，只读快照）
│   └── data.json          # 实时快照（Worker 每 30 分钟自动推送）
├── README.md              # 本文件
└── DEPLOY_STATUS.md       # 部署状态与演进记录
```

---

## 技术栈

| 层 | 技术 |
|---|---|
| **计算引擎** | Cloudflare Python Worker（Pyodide 运行时，`python_workers` + `disable_python_external_sdk` 兼容标志） |
| **数据存储** | Cloudflare Workers KV（快照缓存） |
| **静态面板** | Cloudflare Workers Assets（原生 HTML/JS，无框架；VPN 直连版本） |
| **国内投递** | GitHub Pages（零服务器费，github.io 中国内直连） |
| **数据推送** | Worker → GitHub Contents API（PUT `docs/data.json`） |
| **凭证管理** | GitHub PAT 以 Cloudflare Secret（`GITHUB_TOKEN`）加密存储 |
| **Cron 触发** | Cloudflare Triggers（`45 1 / 15,45 2-7 / 15 8 * * mon-fri`）+ HTTP 自调用 `/api/cron_run` |
| **部署方式** | Cloudflare REST API（multipart PUT）+ `workers-py` SDK |

---

## 部署指南

> **注意**：当前部署通过 Cloudflare REST API 手动进行（`pywrangler` 在某些环境中不可用）。  
> 如需完整 CLI 体验，请使用 `workers-py >= 1.90` + `wrangler`。

```bash
# 1. 安装 workers-py
pip install workers-py>=1.90

# 2. 设置 GitHub PAT（Cloudflare Secret）
# 方式一：Cloudflare Dashboard → Workers → Settings → Variables → Add Secret
# 方式二：REST API PUT 上传时在 metadata.bindings 中包含 {"name":"GITHUB_TOKEN","type":"secret_text"}

# 3. 设置 KV namespace（Cloudflare Dashboard 或 API）

# 4. REST API 部署（本环境使用的实际方式）
# 参考 DEPLOY_STATUS.md，核心是用 multipart PUT 到:
# https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/workers/scripts/market-live
# 包含 metadata (JSON) + worker.py 两部分

# 5. GitHub Pages（仅首次）
# 仓库 Settings → Pages → Source: Deploy from a branch → main → /docs → Save
```

### 必填兼容标志

metadata 中必须包含：

```json
{
  "main_module": "worker.py",
  "compatibility_date": "2026-07-23",
  "compatibility_flags": ["python_workers", "disable_python_external_sdk"]
}
```

其中 `disable_python_external_sdk` 是 Pyodide 运行时的必需标志（`workers-py < 1.90` 兼容）。

---

## 运维诊断（API）

| 端点 | 作用 |
|---|---|
| `/api/data` | 返回当前快照（直接读取 KV，无需重新抓取） |
| `/api/refresh` | 手动刷新：同步构建并写入 KV、推送 GitHub Pages |
| `/api/cron_diag` | 定时触发诊断：返回最近一次 Cron 的状态（`enter` / `dispatched` / `error`），含 `stage`、`at`、`err`、`tb` 字段，用于排查 Cron 问题 |
| `/api/cron_run` | 供 Cron 自调用的内部端点（完整交易日判断 + 刷新 + 诊断留痕） |

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
| **新浪/腾讯** K-line | 上证指数日K（回撤/波动率/动量），腾讯兜底 | 公开 HTTP API |
| **乐咕乐股** legulegu.com | 全市场涨跌/涨停/跌停家数（盘面广度） | 公开页面解析 |
| **蛋卷基金** danjuanfunds.com | 指数估值 PE/PB/分位/股息率 | 公开 HTTP API |
| **Yahoo Finance** | VIX/VXN 隐波 | 公开 API |
| **CNN** dataviz | Fear & Greed Index | 公开 API |
| **GitHub** api.github.com | 投递 data.json 到 Pages 仓库 | OAuth PAT |

> **免责声明**：所有数据均来自公开网络接口，仅供研究参考，不构成投资建议。数据实时性受限于各源更新频率。
