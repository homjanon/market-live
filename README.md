# market-live · 实时市场看板（恐惧指数 / 冰点）

> 国内直连：https://homjanon.github.io/market-live/  
> VPN：https://market-live.homjanon.workers.dev

实时展示 A 股/港股/美股/全球指数、大宗商品、估值水位，并自算**小旭恐惧指数（XXFI）** 与 **A 股冰点**。

---

## 触发规则

| 规则 | 详情 |
|---|---|
| **频率** | 每 30 分钟一次 |
| **首次** | 北京 **9:45** |
| **末次** | 北京 **16:15** |
| **日期** | **仅 A 股/港股交易日**（通过 legu 页面的统计日期自动验证，含调休） |
| **周末** | 跳过 |
| **节假日** | 跳过（春节、国庆等 自动辨识） |

数据由 Cloudflare Cron + 代码内双重守卫保障：
- 时间守卫：`in_trading_window()` 检查 9:30–16:15、周一~周五
- 日期守卫：`is_legu_today()` 检查 legu 页面统计日期是否等于当日，不等说明非交易日

## 数据源

| 数据 | 来源 | 说明 |
|---|---|---|
| A 股/港/美/全球指数 | **东方财富** push2delay | 实时价格、涨跌幅 |
| 大宗商品（WTI/黄金/布伦特） | **新浪** hf\_ | 实时 |
| 离岸人民币 | **东方财富** | USDCNH |
| 估值 PE/PB/股息率 | **蛋卷基金** index_eva | 日级更新 |
| **盘面广度**（涨跌家数） | **乐咕乐股 legu**（主）→ 东财分页（兜底） | 全市场 ≈5198 只（含平盘） |
| 资金流（主力/散户） | **东方财富** ulist.np/get | 净占比口径 |

## 架构

```
┌──────────────────────────────┐
│  Cloudflare Python Worker     │
│  · 采集 9 路数据源           │
│  · 自算 XXFI + 冰点          │
│  · 写入 KV（VPN 版读取）     │
│  · 推 data.json → GitHub     │
│  · Cron 30min · 交易日守卫    │
└──────────────┬───────────────┘
               │
    ┌──────────┴──────────┐
    ▼                      ▼
┌────────────┐    ┌──────────────────┐
│ workers.dev│    │ GitHub Pages     │
│ (VPN 直读) │    │ (国内直连·只读)  │
│ 读 /api/   │    │ 读 ./data.json   │
│ data       │    │ read-only        │
└────────────┘    └──────────────────┘
```

## 文件说明

| 文件 | 作用 |
|---|---|
| `worker.py` | Cloudflare Python Worker 主逻辑（采集+计算+推送） |
| `wrangler.toml` | Worker 部署配置（KV / Cron / Assets） |
| `docs/index.html` | GitHub Pages 前端看板（国内直连版） |
| `docs/data.json` | 实时快照（由 Worker 每 30 分钟自动推送） |
| `public/index.html` | Workers.dev 前端看板（VPN 版，包含手动刷新按钮） |

## 技术栈

- **计算引擎**：Cloudflare Python Worker（Pyodide 运行时）
- **前端**：纯原生 JavaScript（无框架，适配微信内置浏览器与 TTS 朗读）
- **数据投递**：Worker → GitHub API → GitHub Pages（零服务器费、国内直连）
- **凭证**：GitHub PAT 以 Cloudflare Secret 形式加密存储，不进代码
