# -*- coding: utf-8 -*-
"""
market-live · Cloudflare Python Worker
=======================================
实时数据层：自算「小旭恐惧指数(XXFI)」+「A股冰点」，并抓取
A/港/美/全球指数、大宗商品、汇率、估值水位，写入 KV，供前端面板读取。
触发规则详见 wrangler.toml（Cron: 45 1 / 15,45 2-7 / 15 8 * * mon-fri = 北京 9:45–16:15 每 30 分钟，14 次/交易日）。
交易日判断详见 is_tx_today()。

设计要点：
  - 纯 Python（Cloudflare python_workers 运行时），仅用 JS 全局 fetch 出网。
  - 不依赖 akshare / yfinance（Pyodide 装不了原生扩展），全部直连底层 HTTP 端点：
      东财 push2delay（指数/广度/资金流/ETF）、新浪（日K/海外期货）、蛋卷（估值）。
  - 京时间（UTC+8，无夏令时）A股/港股交易日 9:45–16:15 每 30 分钟刷新一次（Cron + 代码双重守卫）。
  - 任一数据源失败均优雅降级，不中断整体。

部署：见 README.md（wrangler deploy）。访问 <worker>.workers.dev 即可。
"""
import json
import re
import base64
import asyncio
from datetime import datetime, timezone, timedelta

from workers import WorkerEntrypoint, Response, fetch as http_fetch, Request as WorkersRequest

KV_KEY = "market_snapshot"
SH_AMT_CACHE_KV_KEY = "_sh_amt_cache"
CRON_DIAG_KV = "_cron_diag"   # 定时触发诊断记录（成功/跳过/崩溃都留痕，便于排查）
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"

# ---------------- 指数 secid 映射（东财 stock/get, fltt=2） ----------------
# 名称 -> (secid, 市场标签)
INDEX_MAP = {
    "上证指数":   ("1.000001", "A"),
    "深证成指":   ("0.399001", "A"),
    "创业板指":   ("0.399006", "A"),
    "沪深300":    ("1.000300", "A"),
    "科创50":     ("1.000688", "A"),
    "北证50":     ("0.899050", "A"),
    "恒生指数":   ("100.HSI",  "H"),
    "恒生科技":   ("124.HSTECH", "H"),
    "纳斯达克100": ("100.NDX",  "US"),
    "标普500":    ("100.SPX",   "US"),
    "道琼斯":     ("100.DJIA",  "US"),
    "日经225":    ("100.N225",  "G"),
    "韩国KOSPI":  ("100.KS11",  "G"),
    "德国DAX":    ("100.GDAXI", "G"),
    "欧洲斯托克600": ("100.SXXP", "G"),
    "法国CAC40":  ("100.FCHI",  "G"),
    "英国富时100": ("100.FTSE",  "G"),
}
# 全球/美股额外展示（已含于上表，这里仅分类）
GROUPS = {
    "A":  ["上证指数", "深证成指", "创业板指", "沪深300", "科创50", "北证50"],
    "H":  ["恒生指数", "恒生科技"],
    "US": ["纳斯达克100", "标普500", "道琼斯"],
    "G":  ["日经225", "韩国KOSPI", "德国DAX", "欧洲斯托克600", "法国CAC40", "英国富时100"],
}
# 大宗商品（新浪 hf_）
COMMODITIES = {
    "WTI原油": "hf_CL",
    "COMEX黄金": "hf_GC",
    "布伦特原油": "hf_OIL",
}
# 离岸人民币（东财）
CNH_SECID = "133.USDCNH"
# 估值（蛋卷 index_eva/dj，按 index_code 抽取）
VALUATION_CODES = {
    "标普500": "SP500",
    "创业板指": "SZ399006",
    "中证红利低波": "CSIH30269",
    "恒生科技": "HKHSTECH",
    "沪深300": "SH000300",
}
# 核心 ETF 篮子（冰点 D2 跌幅占比），对齐 xiaoxu-fear 的 45 只口径。
# 宽基 11 只（含中证2000微盘） + 行业 34 只（含保险/能源/交运/半导体）。
ETF_BASKET = {
    # 宽基 11
    "510300": "沪深300ETF", "510500": "中证500ETF", "159845": "中证1000ETF",
    "159915": "创业板ETF", "588000": "科创50ETF", "510050": "上证50ETF",
    "159901": "深证100ETF", "159338": "中证全指ETF", "159595": "中证100ETF",
    "159628": "国证2000ETF", "563300": "中证2000ETF",
    # 行业 34
    "512000": "证券ETF", "512800": "银行ETF", "512660": "军工ETF",
    "512070": "保险ETF", "588200": "科创板芯片ETF", "512480": "半导体ETF",
    "516160": "新能源ETF", "159755": "电池ETF", "515790": "光伏ETF",
    "512010": "医药ETF", "512170": "医疗ETF", "159928": "消费ETF",
    "512690": "酒ETF", "515170": "食品ETF", "159996": "家电ETF",
    "516110": "汽车ETF", "515220": "煤炭ETF", "512400": "有色金属ETF",
    "159870": "化工ETF", "159825": "农业ETF", "512980": "传媒ETF",
    "159998": "计算机ETF", "515880": "通信ETF", "512200": "地产ETF",
    "515210": "钢铁ETF", "159611": "电力ETF", "516950": "基建ETF",
    "512580": "环保ETF", "159930": "能源ETF", "159666": "交通运输ETF",
    "510880": "红利ETF", "159819": "人工智能ETF", "562500": "机器人ETF",
    "159852": "软件ETF",
}

EM_FS_ALL = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"

# 广度（涨跌家数）主源：乐咕乐股 legu。与 xiaoxu-fear 同源（legu 主 + 新浪备）。
# legu 一次请求给全市场 上涨/下跌/平盘/涨停/跌停，无需分页，远快于东财 clist。
# 注意：legu 不提供「股票总数」字段，故 total 用 up+down+flat（与 xiaoxu-fear 真实口径一致）。
LEGU_URL = "https://www.legulegu.com/stockdata/market-activity"

# GitHub Pages 投递（国内直连）：Worker 算完把 data.json 推到仓库，dashboard 在 github.io 读 ./data.json。
# GITHUB_TOKEN 为 Cloudflare secret（env.GITHUB_TOKEN），不进代码。
GH_REPO = "homjanon/market-live"
GH_DATA_PATH = "docs/data.json"
GH_API = "https://api.github.com"
README_B64 = "IyBtYXJrZXQtbGl2ZSDCtyDlrp7ml7bluILlnLrnnIvmnb8KCj4gKirlm73lhoXnm7Tov54qKu+8mmh0dHBzOi8vaG9tamFub24uZ2l0aHViLmlvL21hcmtldC1saXZlL++8iEdpdEh1YiBQYWdlc++8jOS4reWbveWGheWcsOebtOi/nu+8iSAgCj4gKipWUE4qKu+8mmh0dHBzOi8vbWFya2V0LWxpdmUuaG9tamFub24ud29ya2Vycy5kZXbvvIhDbG91ZGZsYXJlIFdvcmtlcu+8jOWQq+aJi+WKqOWIt+aWsOaMiemSru+8iQoK5a6e5pe25bGV56S6IEEg6IKh44CB5riv6IKh44CB576O6IKh44CB5YWo55CD5Li76KaB5oyH5pWw44CB5aSn5a6X5ZWG5ZOB44CB5rGH546H44CB5Lyw5YC85rC05L2N77yM5bm26Ieq566XKirlsI/ml63mgZDmg6fmjIfmlbDvvIhYWEZJ77yJKiog5LiOICoqQSDogqHlhrDngrkqKuWPguiAg+aMh+agh+OAggoKLS0tCgojIyDmnrbmnoQgwrcg5pWw5o2u5rWBCgpgYGAK4pSM4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSQCuKUgiAgICAgICAgICAgQ2xvdWRmbGFyZSBQeXRob24gV29ya2VyICAgICAgICAgICAgICDilIIK4pSCICAoUHlvZGlkZSDov5DooYzml7YsIENyb24g5q+PMzDliIbpkp/op6blj5EpICAgICAgICAgICAgIOKUggrilIIgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICDilIIK4pSCICDilIzilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilJAgIOKUjOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUkCAg4pSM4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSQICAg4pSCCuKUgiAg4pSCIOS4nOaWuei0ouWvjCAg4pSCICDilIIg5paw5rWq6LSi57uPICDilIIgIOKUgiDkuZDlkpXkuZDogqEgbGVndSAg4pSCICAg4pSCCuKUgiAg4pSCIHB1c2gyZGVsYXnilIIgIOKUgiBzaW5hIGhmXyAg4pSCICDilIIgd3d3LmxlZ3VsZWd1ICAg4pSCICAg4pSCCuKUgiAg4pSCICAgICAgICAgICDilIIgIOKUgiBzaW5hIGtsaW5l4pSCICDilIIgLmNvbSAgICAgICAgICAg4pSCICAg4pSCCuKUgiAg4pSU4pSA4pSA4pSA4pSA4pSs4pSA4pSA4pSA4pSA4pSA4pSYICDilJTilIDilIDilIDilIDilKzilIDilIDilIDilIDilIDilJggIOKUlOKUgOKUgOKUgOKUgOKUgOKUgOKUrOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUmCAgIOKUggrilIIgICAgICAg4pSCICAgICAgICAgICAgIOKUgiAgICAgICAgICAgICAgIOKUgiAgICAgICAgICAgIOKUggrilIIgICAgICAg4pa8ICAgICAgICAgICAgIOKWvCAgICAgICAgICAgICAgIOKWvCAgICAgICAgICAgIOKUggrilIIgIOKUjOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUkCAgICAg4pSCCuKUgiAg4pSCICAxMCDot6/lubblj5HmipPlj5Yg4oaSIGJ1aWxkX3NuYXBzaG90KCkgICAgICAgIOKUgiAgICAg4pSCCuKUgiAg4pSCICDCtyDmjIfmlbAv5rGH546HL+WVhuWTgSDCtyDlub/luqYv6LWE6YeR5rWBL+S8sOWAvCAgICDilIIgICAgIOKUggrilIIgIOKUgiAgwrcg6Ieq566XIFhYRkkgKyDlhrDngrkgICAgICAgICAgICAgICAgICAgIOKUgiAgICAg4pSCCuKUgiAg4pSU4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSs4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSYICAgICDilIIK4pSCICAgICAgICAgICAgICAgICAgIOKUgiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIOKUggrilIIgICAgICAgICAg4pSM4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pS04pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSQICAgICAgICAgICAgICAgICAgICAg4pSCCuKUgiAgICAgICAgICDilrwgICAgICAgICAgICAgICAgIOKWvCAgICAgICAgICAgICAgICAgICAgICDilIIK4pSCICDilIzilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilJAgIOKUjOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUkCAgICAgICAgICAgICAg4pSCCuKUgiAg4pSCIEtWICh3b3JrZXJzIOKUgiAg4pSCIEdpdEh1YiBBUEkgICAg4pSCICAgICAgICAgICAgICDilIIK4pSCICDilIIgIC5kZXYg6K+75Y+WKSAg4pSCICDilIIgUFVUIGRhdGEuanNvbiDilIIgICAgICAgICAgICAgIOKUggrilIIgIOKUlOKUgOKUgOKUgOKUgOKUgOKUgOKUrOKUgOKUgOKUgOKUgOKUgOKUgOKUmCAg4pSU4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSs4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSYICAgICAgICAgICAgICDilIIK4pSU4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pS84pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pS84pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSYCiAgICAgICAgICDilIIgICAgICAgICAgICAgICAgIOKUggogICAgICAgICAg4pa8ICAgICAgICAgICAgICAgICDilrwK4pSM4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSQICDilIzilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilJAK4pSCIHdvcmtlcnMuZGV2ICAgICDilIIgIOKUgiBHaXRIdWIgUGFnZXMgICAgICAgICAgICAgIOKUggrilIIgKOmcgFZQTikgICAgICAgICDilIIgIOKUgiBnaXRodWIuaW8gKOWbveWGheebtOi/nikgICAgICDilIIK4pSCIOivuyAvYXBpL2RhdGEgICAg4pSCICDilIIg6K+7IC4vZGF0YS5qc29uICjlj6ror7spICAgICDilIIK4pSCIOWQq+aJi+WKqOWIt+aWsOaMiemSriAgIOKUgiAg4pSCIOaXoOaJi+WKqOWIt+aWsCAgICAgICAgICAgICAgICDilIIK4pSU4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSYICDilJTilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilJgKYGBgCgoqKuaguOW/g+mAu+i+kSoq77yaV29ya2VyIOavjzMw5YiG6ZKf6YeH6ZuGMTDot6/mlbDmja7mupAg4oaSIOiuoeeulyBYWEZJICsg5Yaw54K5IOKGkiDlkIzml7blhpnlhaUgS1bvvIjkvpsgVlBOIOeJiOivu+WPlu+8ieWSjOaOqOmAgeWIsCBHaXRIdWIgUGFnZXPvvIjkvpvlm73lhoXnm7Tov57vvInjgIIKCi0tLQoKIyMg6Kem5Y+R6KeE5YiZCgp8IOinhOWImSB8IOWAvCB8CnwtLS18LS0tfAp8ICoqQ3Jvbioq77yIVVRD77yJIHwgYDQ1IDFgIC8gYDE1LDQ1IDItN2AgLyBgMTUgOGAgYCogKiBtb24tZnJpYO+8iDMg5q6177yM5YWxIDE0IOasoS/kuqTmmJPml6XvvIkgfAp8ICoqQ3Jvbioq77yI5YyX5Lqs77yJIHwgYDk6NDUsIDEwOjE1LCAxMDo0NSwg4oCmLCAxNjoxNWDvvIjmr48gMzAg5YiG6ZKf77yJIHwKfCAqKummluasoeinpuWPkSoqIHwg5YyX5LqsICoqOTo0NSoqIHwKfCAqKuacq+asoeinpuWPkSoqIHwg5YyX5LqsICoqMTY6MTUqKiB8CnwgKirkuqTmmJPml6UqKiB8ICoqQSDogqHkuqTmmJPml6UqKu+8iOmAmui/hyBsZWd1IOmhtemdoue7n+iuoeaXpeacn+iHquWKqOagoemqjO+8jOWQq+iwg+S8ke+8iSB8CnwgKirlkajmnKsqKiB8IOKdjCDot7Pov4cgfAp8ICoq6IqC5YGH5pelKiogfCDinYwg6Lez6L+H77yI5pil6IqCL+WbveW6hi/muIXmmI7nrYnvvIkgfAoK5Y+M6YeN5a6I5Y2r77yaCi0gYGluX3RyYWRpbmdfd2luZG93KClgIOKAlCDljJfkuqzml7YgOTozMOKAkzE2OjE144CB5ZGo5LiAfuWRqOS6lAotIGBpc190eF90b2RheSgpYCDigJQg5qOA5p+lIGxlZ3Ug6aG16Z2i57uf6K6h5pel5pyf5piv5ZCm562J5LqO5b2T5pelCgotLS0KCiMjIOaVsOaNruadv+WdlwoKIyMjIPCfk4ggQSDogqHvvIg2IOWPqu+8iQoKfCDmjIfmlbAgfCDmnaXmupAgfCBzZWNpZCB8CnwtLS18LS0tfC0tLXwKfCDkuIror4HmjIfmlbAgfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxLjAwMDAwMWAgfAp8IOa3seivgeaIkOaMhyB8IOS4nOaWuei0ouWvjCBwdXNoMmRlbGF5IHwgYDAuMzk5MDAxYCB8Cnwg5Yib5Lia5p2/5oyHIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMC4zOTkwMDZgIHwKfCDmsqrmt7EzMDAgfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxLjAwMDMwMGAgfAp8IOenkeWImzUwIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMS4wMDA2ODhgIHwKfCAqKuWMl+ivgTUwKiogfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAwLjg5OTA1MGAgfAoKIyMjIPCfk4og5riv6IKh77yIMiDlj6rvvIkKCnwg5oyH5pWwIHwg5p2l5rqQIHwgc2VjaWQgfAp8LS0tfC0tLXwtLS18Cnwg5oGS55Sf5oyH5pWwIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTAwLkhTSWAgfAp8IOaBkueUn+enkeaKgCB8IOS4nOaWuei0ouWvjCBwdXNoMmRlbGF5IHwgYDEyNC5IU1RFQ0hgIHwKCiMjIyDwn4e68J+HuCDnvo7ogqHvvIgzIOWPqu+8iQoKfCDmjIfmlbAgfCDmnaXmupAgfCBzZWNpZCB8CnwtLS18LS0tfC0tLXwKfCDnurPmlq/ovr7lhYsxMDAgfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxMDAuTkRYYCB8Cnwg5qCH5pmuNTAwIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTAwLlNQWGAgfAp8IOmBk+eQvOaWryB8IOS4nOaWuei0ouWvjCBwdXNoMmRlbGF5IHwgYDEwMC5ESklBYCB8CgojIyMg8J+MjSDlhajnkIPvvIg2IOWPqu+8iQoKfCDmjIfmlbAgfCDmnaXmupAgfCBzZWNpZCB8CnwtLS18LS0tfC0tLXwKfCDml6Xnu48yMjUgfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxMDAuTjIyNWAgfAp8IOmfqeWbvUtPU1BJIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTAwLktTMTFgIHwKfCDlvrflm71EQVggfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxMDAuR0RBWElgIHwKfCAqKuasp+a0suaWr+aJmOWFizYwMCoqIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTAwLlNYWFBgIHwKfCAqKuazleWbvUNBQzQwKiogfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxMDAuRkNISWAgfAp8ICoq6Iux5Zu95a+M5pe2MTAwKiogfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxMDAuRlRTRWAgfAoKIyMjIPCfm6LvuI8g5aSn5a6X5ZWG5ZOB77yIMyDlj6rvvIkKCnwg5ZOB56eNIHwg5p2l5rqQIHwgc3ltYm9sIHwKfC0tLXwtLS18LS0tfAp8IFdUSSDljp/msrkgfCDmlrDmtaogaGZcXyB8IGBoZl9DTGAgfAp8IENPTUVYIOm7hOmHkSB8IOaWsOa1qiBoZlxfIHwgYGhmX0dDYCB8Cnwg5biD5Lym54m55Y6f5rK5IHwg5paw5rWqIGhmXF8gfCBgaGZfT0lMYCB8CgojIyMg8J+SsSDmsYfnjofvvIgxIOWPqu+8iQoKfCDlk4Hnp40gfCDmnaXmupAgfCBzZWNpZCB8CnwtLS18LS0tfC0tLXwKfCDnvo7lhYPnprvlsrjkurrmsJHluIEgfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxMzMuVVNEQ05IYCB8CgotLS0KCiMjIOiuoeeul+aMh+aghwoKIyMjIOWwj+aXreaBkOaDp+aMh+aVsO+8iFhYRknvvIkKCuaBkOaDpyA0IOmhuSArIOi0quWpqiA1IOmhue+8jOWKoOadg+WQiOaIkOaBkOaDp+aMh+aVsO+8iFhYRkkgPSDmgZDmg6fmgLvliIbvvInjgILkuI4gW3hpYW94dS1mZWFyXShodHRwczovL2dpdGh1Yi5jb20vaG9tamFub24veGlhb3h1LWZlYXIpIOWujOWFqOWQjOa6kOiuoeeul+OAggoKKirmgZDmg6fliIbph4/vvIjlgLzotorpq5jihpLotormgZDmg6fihpLlj43lkJHnnIvlpJrvvInvvJoqKgoKfCDliIbph48gfCDmnYPph40gfCDlhazlvI8gfCDmlbDmja7mnaXmupAgfAp8LS0tfC0tLXwtLS18LS0tfAp8IOWbnuaSpCBkcmF3ZG93biB8IDAuMzAgfCBjbGFtcChhYnMoZGQpw5c1MDApIHwg5LiK6K+B5oyH5pWwIDIwIOaXpSBLICsg5b2T5pelIHNwb3TvvIjmlrDmtarvvIkgfAp8IOW5v+W6piBicmVhZHRoIHwgMC4yNSB8IGNsYW1wKChkb3duL3VwLTAuNSnDlzEwMCkgfCDkuZDlkpXkuZDogqEgbGVnde+8iOWFqOW4guWcuua2qOi3jOWutuaVsO+8iSB8Cnwg6LeM5YGc5q+UIGxpbWl0ZG93biB8IDAuMjAgfCBjbGFtcCgobGQvbHUpw5c1MCkgfCDkuZDlkpXkuZDogqEgbGVnde+8iOa2qOWBnC/ot4zlgZzlrrbmlbDvvIkgfAp8IOazouWKqOeOhyB2b2wgfCAwLjI1IHwgY2xhbXAodm9sX3BjdMOXMTAwKSB8IOS4iuivgeaMh+aVsCAyMCDml6Xms6LliqjnjofliIbkvY3vvIjmlrDmtarvvIkgfAoKKirotKrlqarliIbph4/vvIjlgLzotorpq5jihpLotorotKrlqarihpLlj43lkJHnnIvnqbrvvInvvJoqKgoKfCDliIbph48gfCDmnYPph40gfCDlhazlvI8gfCDmlbDmja7mnaXmupAgfAp8LS0tfC0tLXwtLS18LS0tfAp8IOWKqOmHjyBtb21lbnR1bSB8IDAuMjUgfCBjbGFtcChyZXQyMMOXNTAwKSB8IOS4iuivgeaMh+aVsCAyMCDml6XmtqjluYXvvIjmlrDmtarvvIkgfAp8IOa2qOWBnOavlCBsaW1pdHVwIHwgMC4xNSB8IGNsYW1wKChsdS9sZCnDlzUwKSB8IOS5kOWSleS5kOiCoSBsZWd1IHwKfCDmlaPmiLfov73pq5ggcmV0YWlsaW4gfCAwLjIwIHwgY2xhbXAocmV0YWlsX25ldMOXMjAwKSB8IOS4nOaWuei0ouWvjCB1bGlzdO+8iOayqua3seS4pOW4gui1hOmHkea1ge+8iSB8Cnwg6LaF5LmwIG92ZXJib3VnaHQgfCAwLjIwIHwgY2xhbXAoYWJvdmXDlzUwMCkgfCDkuIror4HmjIfmlbDpq5jkuo4gMjAg5pel5Z2H57q/5bmF5bqmIHwKfCDog4znprsgZGl2ZXJnZW5jZSB8IDAuMjAgfCBjbGFtcCg1MOKIkmRpdsOXMjAwKSB8IOS4u+WKm+KIkuaVo+aIt++8iOS4nOi0oui1hOmHkea1ge+8iSB8CgoqKlhYRkkg5L+h5Y+36ZiI5YC8KirvvJoKCnwgWFhGSSB8IOS/oeWPtyB8IOWQq+S5iSB8CnwtLS18LS0tfC0tLXwKfCDiiaU3NSB8ICoqQlVZKiogfCDmnoHluqbmgZDmg6fvvIzliIbmibnkvY7lkLggfAp8IDYw4oCTNzQgfCAqKkFDQ1VNVUxBVEUqKiB8IOaBkOaDp++8jOmAouS9juWQuOe6syB8CnwgNDDigJM1OSB8ICoqSE9MRCoqIHwg5Lit5oCn77yM5oyJ562W55Wl5oyB5pyJIHwKfCAyNeKAkzM5IHwgKipSRURVQ0UqKiB8IOWBj+i0quWpqu+8jOmAoumrmOWHj+S7kyB8CnwgPDI1IHwgKipTRUxMKiogfCDmnoHluqbotKrlqarvvIzlh4/ku5Ppgb/pmakgfAoKIyMjIEEg6IKh5Yaw54K577yINCDnu7TluqbvvIzlhajpg6jmu6HotrMgPSDlhrDngrnvvIkKCnwg57u05bqmIHwg6ZiI5YC8IHwg5pWw5o2u5rqQIHwKfC0tLXwtLS18LS0tfAp8IEQxIOS4i+i3jOW5v+W6piB8IOS4i+i3jCDiiaUgNDAwMCDkuJQg5Y2g5q+UIOKJpSA4NSUgfCBsZWd1IOWFqOW4guWcuua2qOi3jOWutuaVsCB8CnwgRDIg5oyH5pWwL0VURiDot4zluYUgfCDkuIror4Eg4omkIC0yLjAlIOS4lCDliJvkuJrmnb8g4omkIC0yLjUlIOS4lCBFVEYg6LeMIOKJpCAtMi41JSDljaDmr5Qg4omlIDYwJSB8IOS4nOi0ouaMh+aVsCArIDQ1IOWPquaguOW/gyBFVEYg55m95ZCN5Y2VIHwKfCBEMyDot4zlgZzmlbDph48gfCDot4zlgZwg4omlIDUwIOS4lCDot4zlgZwv5rao5YGcIOKJpSAzIHwgbGVndSDmtqjlgZwv6LeM5YGc5a625pWwIHwKfCBENCDmlL7ph4/mgZDmhYwgfCDmlL7ph4/lgI3mlbDvvIjlvZPml6XmiJDkuqTpop0v6L+RIDIwIOaXpeWdh+mine+8ieKJpSAxLjMgfCDkuJzotKIgdWxpc3TvvIjlvZPml6Xpop3vvIkrIEtWIOa7muWKqOe8k+WtmO+8iDIw5pel5Z2H6aKd77yM6aaW5qyh5LuO6IW+6K6vbmV3ZnFrbGluZeihpem9kO+8iSB8CgrmoLjlv4MgRVRGIOeZveWQjeWNleWFsSAqKjQ1IOWPqioq77yI5a695Z+6IDExIOWPqiArIOihjOS4miAzNCDlj6rvvIzlkKvkuK3or4EyMDAwIOW+ruebmOOAgeS/nemZqS/og73mupAv5Lqk6L+QL+WNiuWvvOS9k+etieihpeWFhemhue+8ieOAggoKIyMjIPCfh7rwn4e4IOe+juiCoeaBkOaFjOaMh+aVsAoK5ZyoIEEg6IKh5Yaw54K55Y2h5LiL5pa55bGV56S677yM5YyF5ZCr5LiJ5Liq5oyH5qCH77yaCgp8IOaMh+aghyB8IOWQq+S5iSB8IOaVsOaNrua6kCB8IOino+ivu+WMuumXtCB8CnwtLS18LS0tfC0tLXwtLS18CnwgKipWSVgqKiB8IOagh+aZrjUwMCAzMOWkqemakOazoiB8IFlhaG9vIEZpbmFuY2UgfCDiiaU0MOaegeW6puaBkOaFjCAvIOKJpTMw5oGQ5oWMIC8g4omlMjXlgY/mgZDmhYwgLyDiiaUyMOWBj+mrmCAvIOKJpTE15q2j5bi4IC8g4omlMTLlgY/kvY4gLyA8MTLmnoHkvY7otKrlqaogfAp8ICoqVlhOKiogfCDnurPmlq/ovr7lhYsxMDAgMzDlpKnpmpDms6IgfCBZYWhvbyBGaW5hbmNlIHwg5ZCM5LiK77yI57qz5oyH5rOi5Yqo546H6YCa5bi46auY5LqO5qCH5pmu77yJIHwKfCAqKkNOTiBGZWFyICYgR3JlZWQqKiB8IDflm6DlrZDmgZDmg6fotKrlqarmjIfmlbDvvIgw4oCTMTAw77yJIHwgQ05OIGRhdGF2aXogQVBJIHwg4omkMjXmnoHluqbmgZDmhYwgLyDiiaQ0NeaBkOaFjCAvIOKJpDU15Lit5oCnIC8g4omkNzXotKrlqaogLyA+NzXmnoHluqbotKrlqaogfAoKIyMjIOS8sOWAvOawtOS9jQoKfCDmjIfmlbAgfCDmnaXmupAgfAp8LS0tfC0tLXwKfCDmoIfmma41MDAgfCDom4vljbfln7rph5EgaW5kZXhfZXZhIHwKfCDliJvkuJrmnb/mjIcgfCDom4vljbfln7rph5EgaW5kZXhfZXZhIHwKfCDkuK3or4HnuqLliKnkvY7ms6IgfCDom4vljbfln7rph5EgaW5kZXhfZXZhIHwKfCDmgZLnlJ/np5HmioAgfCDom4vljbfln7rph5EgaW5kZXhfZXZhIHwKfCDmsqrmt7EzMDAgfCDom4vljbfln7rph5EgaW5kZXhfZXZhIHwKCi0tLQoKIyMg5paH5Lu257uT5p6ECgpgYGAKbWFya2V0LWxpdmUvCuKUnOKUgOKUgCB3b3JrZXIucHkgICAgICAgICAgICAgICMgQ2xvdWRmbGFyZSBQeXRob24gV29ya2VyIOS4u+mAu+i+kQrilIIgICDilJzilIAg6YeH6ZuGIDkg6Lev5pWw5o2u5rqQICAgICAjIChIVFRQIOW5tuWPkSwgYXN5bmNpby5nYXRoZXIpCuKUgiAgIOKUnOKUgCBjb21wdXRlX3h4ZmkoKSAgICAgICMg5bCP5pet5oGQ5oOn5oyH5pWw6K6h566XCuKUgiAgIOKUnOKUgCBjb21wdXRlX2JpbmdkaWFuKCkgICMgQSDogqHlhrDngrnliKTlrpoK4pSCICAg4pSc4pSAIHB1Ymxpc2hfdG9fZ2l0aHViKCkgIyDmjqggZGF0YS5qc29uIOWIsCBHaXRIdWIK4pSCICAg4pSU4pSAIGNsYXNzIERlZmF1bHQgICAgICAgIyBXb3JrZXIg5YWl5Y+jIChmZXRjaCArIHNjaGVkdWxlZCkK4pSc4pSA4pSAIHdyYW5nbGVyLnRvbWwgICAgICAgICAgIyBDbG91ZGZsYXJlIOmDqOe9sumFjee9rgrilIIgICDilJzilIAgS1Yg57uR5a6aICAgICAgICAgICAgICMg5b+r54Wn5a2Y5YKoCuKUgiAgIOKUnOKUgCBBU1NFVFMg57uR5a6aICAgICAgICAgIyDpnZnmgIHpnaLmnb8K4pSCICAg4pSU4pSAIENyb24g6Kem5Y+RICAgICAgICAgICAjIDQ1IDEgLyAxNSw0NSAyLTcgLyAxNSA4ICogKiBtb24tZnJpCuKUnOKUgOKUgCBwdWJsaWMvCuKUgiAgIOKUlOKUgOKUgCBpbmRleC5odG1sICAgICAgICAgIyBXb3JrZXIg54mI5YmN56uv77yIVlBOIOWPr+eUqO+8jOWQq+aJi+WKqOWIt+aWsOaMiemSru+8iQrilJzilIDilIAgZG9jcy8gICAgICAgICAgICAgICAgICAjIEdpdEh1YiBQYWdlcyDmipXpgJLnm67lvZUK4pSCICAg4pSc4pSA4pSAIGluZGV4Lmh0bWwgICAgICAgICAjIEdpdEh1YiBQYWdlcyDniYjliY3nq6/vvIjlm73lhoXnm7Tov57vvIzlj6ror7vlv6vnhafvvIkK4pSCICAg4pSU4pSA4pSAIGRhdGEuanNvbiAgICAgICAgICAjIOWunuaXtuW/q+eFp++8iFdvcmtlciDmr48gMzAg5YiG6ZKf6Ieq5Yqo5o6o6YCB77yJCuKUnOKUgOKUgCBweXByb2plY3QudG9tbCAgICAgICAgICMgUHl0aG9uIOmhueebrumFjee9rgrilJzilIDilIAgcHlsb2NrLnRvbWwgICAgICAgICAgICAjIHV2IOS+nei1lumUgQrilJzilIDilIAgUkVBRE1FLm1kICAgICAgICAgICAgICAjIOacrOaWh+S7tgrilJTilIDilIAgREVQTE9ZX1NUQVRVUy5tZCAgICAgICAjIOmDqOe9sueKtuaAgeS4jua8lOi/m+iusOW9lQpgYGAKCi0tLQoKIyMg5oqA5pyv5qCICgp8IOWxgiB8IOaKgOacryB8CnwtLS18LS0tfAp8ICoq6K6h566X5byV5pOOKiogfCBDbG91ZGZsYXJlIFB5dGhvbiBXb3JrZXLvvIhQeW9kaWRlIOi/kOihjOaXtu+8jGBweXRob25fd29ya2Vyc2Ag5YW85a655qCH5b+X77yJIHwKfCAqKuaVsOaNruWtmOWCqCoqIHwgQ2xvdWRmbGFyZSBXb3JrZXJzIEtW77yI5b+r54Wn57yT5a2Y77yJIHwKfCAqKumdmeaAgemdouadvyoqIHwgQ2xvdWRmbGFyZSBXb3JrZXJzIEFzc2V0c++8iOWOn+eUnyBIVE1ML0pT77yM5peg5qGG5p6277yJIHwKfCAqKuWbveWGheaKlemAkioqIHwgR2l0SHViIFBhZ2Vz77yI6Zu25pyN5Yqh5Zmo6LS577yMZ2l0aHViLmlvIOS4reWbveWGheebtOi/nu+8iSB8CnwgKirmlbDmja7mjqjpgIEqKiB8IFdvcmtlciDihpIgR2l0SHViIENvbnRlbnRzIEFQSSDihpIgYGRvY3MvZGF0YS5qc29uYCB8CnwgKirlh63or4HnrqHnkIYqKiB8IEdpdEh1YiBQQVQg5LulIENsb3VkZmxhcmUgU2VjcmV0IOW9ouW8j+WKoOWvhuWtmOWCqO+8jOS4jei/m+S7o+eggSB8CnwgKipDcm9uKiogfCBDbG91ZGZsYXJlIFRyaWdnZXJz77yI5q+PIDMwIOWIhumSn++8iSB8CnwgKirpg6jnvbLlt6XlhbcqKiB8IGBweXdyYW5nbGVyYO+8iHdvcmtlcnMtcHkg4omlMS45MO+8ieKGkiBgd3JhbmdsZXIgZGVwbG95YCB8CgotLS0KCiMjIOmDqOe9suaMh+WNlwoKYGBgYmFzaAojIDEuIOWuieijheS+nei1lgp1diBzeW5jCgojIDIuIOiuvue9riBHaXRIdWIgUEFU77yI55So5LqO5pWw5o2u5o6o6YCB77yJCmVjaG8gIuS9oOeahGdpdGh1Yl90b2tlbiIgfCBDTE9VREZMQVJFX0FQSV9UT0tFTj0i5L2g55qEY2ZfdG9rZW4iIENMT1VERkxBUkVfQUNDT1VOVF9JRD0i5L2g55qEY2ZfYWNjb3VudF9pZCIgdXYgcnVuIHB5d3JhbmdsZXIgc2VjcmV0IHB1dCBHSVRIVUJfVE9LRU4KCiMgMy4g6YOo572yIFdvcmtlcgpDTE9VREZMQVJFX0FQSV9UT0tFTj0iLi4uIiBDTE9VREZMQVJFX0FDQ09VTlRfSUQ9Ii4uLiIgdXYgcnVuIHB5d3JhbmdsZXIgZGVwbG95CgojIDQuIEdpdEh1YiBQYWdlcyDorr7nva7vvIjku4XpppbmrKHvvIkKIyDku5PlupMgU2V0dGluZ3Mg4oaSIFBhZ2VzIOKGkiBTb3VyY2U6IERlcGxveSBmcm9tIGJyYW5jaCDihpIgbWFpbiDihpIgL2RvY3Mg4oaSIFNhdmUKYGBgCgotLS0KCiMjIOi/kOe7tOiviuaWre+8iEFQSe+8iQoKfCDnq6/ngrkgfCDkvZznlKggfAp8LS0tfC0tLXwKfCBgL2FwaS9kYXRhYCB8IOi/lOWbnuW9k+WJjeW/q+eFp++8iOebtOaOpeivu+WPliBLVu+8jOaXoOmcgOmHjeaWsOaKk+WPlu+8iSB8CnwgYC9hcGkvcmVmcmVzaGAgfCDmiYvliqjliLfmlrDvvJrlkIzmraXmnoTlu7rlubblhpnlhaUgS1bjgIHmjqjpgIEgR2l0SHViIFBhZ2Vz77yI562J5Lu35LqOIENyb24g6LeR55qE6YC76L6R77yJIHwKfCBgL2FwaS9jcm9uX2RpYWdgIHwg5a6a5pe26Kem5Y+R6K+K5pat77ya6L+U5Zue5pyA6L+R5LiA5qyhIENyb24g55qE54q25oCB77yIYGVudGVyYCAvIGBkaXNwYXRjaGVkYCAvIGBlcnJvcmDvvInvvIzlkKsgYGluX3dpbmRvd2DjgIFgaXNfdHhg44CBYGVycmDjgIFgdGJgIOWtl+aute+8jOeUqOS6juaOkuafpeKAnOiHquWKqOinpuWPkeayoei3keKAnemXrumimCB8CgoqKkNyb24g5YGl5aOu5oCnKirvvJpgc2NoZWR1bGVkKClgIOmHh+eUqCBgY29udHJvbGxlci53YWl0VW50aWxgIOS8mOWFiOOAgeWbnumAgCBgc2VsZi5jdHgud2FpdFVudGlsYOOAgeacgOe7iOWFnOW6leebtOaOpSBgYXdhaXRgIOeahOS4iemHjeWGmeazle+8jOS4lOWFqOeoiyBgdHJ5L2V4Y2VwdGAg5oqK5byC5bi45YaZ5YWlIEtW77yIYF9jcm9uX2RpYWdg77yJ77yMKirkuI3lho3pnZnpu5jlpLHotKUqKuOAggoKLS0tCgojIyDkuI4geGlhb3h1LWZlYXIg55qE5YWz57O7CgrmnKzpobnnm67nmoQgWFhGSSDorqHnrpfkuI4gW3hpYW94dS1mZWFyXShodHRwczovL2dpdGh1Yi5jb20vaG9tamFub24veGlhb3h1LWZlYXIpIOWujOWFqOWQjOa6kO+8iOWFrOW8j+OAgeadg+mHjeOAgemYiOWAvOOAgeaWh+acrOmAkOihjOS4gOiHtO+8ie+8jOW3ruW8guS7heWcqOS6ju+8mgoKLSB4aWFveHUtZmVhciDkvb/nlKggYWtzaGFyZSDlj5bmlbDvvIjmnKzlnLAvR2l0SHViIEFjdGlvbnPvvIkKLSBtYXJrZXQtbGl2ZSDkvb/nlKjnm7Tov54gSFRUUCBBUEkg5Y+W5pWw77yIQ2xvdWRmbGFyZSBXb3JrZXIg5pegIGFrc2hhcmXvvIkKLSB4aWFveHUtZmVhciDovpPlh7rkuLogR2l0SHViIFBhZ2VzIOmdmeaAgeaKpeWRigotIG1hcmtldC1saXZlIOS4uuWunuaXtua7muWKqOeci+adv++8iOavjyAzMCDliIbpkp/liLfmlrDvvIkKCi0tLQoKIyMg5pWw5o2u5p2l5rqQCgp8IOadpea6kCB8IOeUqOmAlCB8IOWNj+iuriB8CnwtLS18LS0tfC0tLXwKfCAqKuS4nOaWuei0ouWvjCoqIHB1c2gyZGVsYXkgfCBBL+a4ry/nvo4v5YWo55CD5oyH5pWw44CB5rGH546H44CB6LWE6YeR5rWB44CBRVRG44CBSy1saW5l77yI5oiQ5Lqk6aKd77yJIHwg5YWs5byAIEhUVFAgQVBJIHwKfCAqKuaWsOa1qui0oue7jyoqIHNpbmEgaGZcXyB8IOWFqOeQg+Wkp+Wul+WVhuWTgeWunuaXtuihjOaDhSB8IOWFrOW8gCBIVFRQIEFQSSB8CnwgKirmlrDmtarotKLnu48qKiBLLWxpbmUgQVBJIHwg5LiK6K+B5oyH5pWw5pelS++8iOWbnuaSpC/ms6Lliqjnjocv5Yqo6YeP77yJIHwg5YWs5byAIEhUVFAgQVBJIHwKfCAqKuS5kOWSleS5kOiCoSoqIGxlZ3VsZWd1LmNvbSB8IOWFqOW4guWcuua2qOi3jC/mtqjlgZwv6LeM5YGc5a625pWw77yI55uY6Z2i5bm/5bqm77yJIHwg5YWs5byA6aG16Z2i6Kej5p6QIHwKfCAqKuibi+WNt+WfuumHkSoqIGRhbmp1YW5mdW5kcy5jb20gfCDmjIfmlbDkvLDlgLwgUEUvUEIv5YiG5L2NL+iCoeaBr+eOhyB8IOWFrOW8gCBIVFRQIEFQSSB8CnwgKipHaXRIdWIqKiBhcGkuZ2l0aHViLmNvbSB8IOaKlemAkiBkYXRhLmpzb24g5YiwIFBhZ2VzIOS7k+W6kyB8IE9BdXRoIFBBVCB8Cgo+ICoq5YWN6LSj5aOw5piOKirvvJrmiYDmnInmlbDmja7lnYfmnaXoh6rlhazlvIDnvZHnu5zmjqXlj6PvvIzku4XkvpvnoJTnqbblj4LogIPvvIzkuI3mnoTmiJDmipXotYTlu7rorq7jgILmlbDmja7lrp7ml7bmgKflj5fpmZDkuo7lkITmupDmm7TmlrDpopHnjofjgIIK"


# ============================ HTTP 工具 ============================
async def http_get(url, ref="https://quote.eastmoney.com/"):
    """异步 GET，返回文本。模拟浏览器 UA + Referer，规避东财/新浪防盗链。
    注意：workers.fetch 用 **kwargs 语义，必须传 headers={...} 关键字参数，
    不能传 {"headers": {...}} 位置字典（会被误解析为 headers={"headers": {...}}）。
    用 asyncio.wait_for 加超时，避免边缘环境某源挂起拖垮整体。"""
    try:
        resp = await asyncio.wait_for(
            http_fetch(url, headers={"User-Agent": UA, "Referer": ref}), timeout=12)
        return await resp.text()
    except Exception as e:
        return json.dumps({"_error": str(e)})


def jload(text):
    try:
        return json.loads(text)
    except Exception:
        return {}


# ============================ 抓取器 ============================
async def _fetch_one_index(name, secid, market):
    url = (f"https://push2delay.eastmoney.com/api/qt/stock/get?fltt=2&invt=2&"
           f"secid={secid}&fields=f58,f43,f170,f86")
    d = jload(await http_get(url))
    q = d.get("data") or {}
    return name, {
        "name": name, "market": market, "secid": secid,
        "price": q.get("f43"), "chg": q.get("f170"), "ts": q.get("f86"),
    }


async def fetch_index_quotes():
    """东财 stock/get 并发取指数/汇率最新价与涨跌幅。"""
    tasks = [_fetch_one_index(n, s, m) for n, (s, m) in INDEX_MAP.items()]
    # 离岸人民币
    tasks.append(_fetch_one_index("美元离岸人民币", CNH_SECID, "FX"))
    out = {}
    for name, rec in await asyncio.gather(*tasks):
        out[name] = rec
    return out


async def fetch_commodities():
    """新浪海外期货 hf_。"""
    syms = ",".join(COMMODITIES.values())
    url = f"https://hq.sinajs.cn/list={syms}"
    txt = await http_get(url, ref="https://finance.sina.com.cn")
    out = {}
    for line in txt.split(";"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, val = line.split("=", 1)
        sym = key.replace("var hq_str_", "").strip()
        name = {v: k for k, v in COMMODITIES.items()}.get(sym)
        if not name:
            continue
        parts = [p.strip() for p in val.strip('"').split(",")]
        # 新浪 hf_ 外盘期货字段顺序（名称在末尾，无名称前缀）：
        # [0]买价 [1]卖价 [2]? [3]最新价 [4]最高 [5]最低
        # [6]时间 [7]今开 [8]昨收 [9..]量 [12]日期 [13]名称
        try:
            last = float(parts[3])
            prev = float(parts[8]) if len(parts) > 8 and parts[8] else None
            chg = round((last / prev - 1) * 100, 2) if prev else None
        except (ValueError, IndexError):
            last, chg = None, None
        out[name] = {"name": name, "price": last, "chg": chg,
                     "source": "sina hf_"}
    return out


async def fetch_sina_kline(symbol, n):
    """新浪日K：返回 [{day, close, volume}, ...]。"""
    url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           f"CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen={n}")
    d = jload(await http_get(url, ref="https://finance.sina.com.cn"))
    if not isinstance(d, list):
        return []
    return [{"day": r.get("day"), "close": float(r["close"]),
            "volume": float(r.get("volume", 0))} for r in d if r.get("close")]


def max_drawdown(prices):
    peak, mdd = prices[0], 0.0
    for p in prices:
        if p > peak:
            peak = p
        dd = (p - peak) / peak
        if dd < mdd:
            mdd = dd
    return mdd


def roll_vol(prices, w=20):
    vols = []
    for i in range(w, len(prices)):
        seg = prices[i - w:i]
        rets = [seg[j] / seg[j - 1] - 1 for j in range(1, len(seg))]
        m = sum(rets) / len(rets)
        var = sum((r - m) ** 2 for r in rets) / len(rets)
        vols.append(var ** 0.5)
    return vols


async def fetch_hs300_deriv():
    """上证指数 近20日回撤/动量/均线偏离/波动率分位（XXFI 输入），对齐 xiaoxu-fear。
    源链：新浪日K(主) → 腾讯日K proxy.finance.qq.com(兜底，Cloudflare 边缘可达)。
    当日补点：日K末根滞后约1天，用新浪实时 spot 补当日一根。"""
    closes, last_day = None, None
    # 主源：新浪
    try:
        kl = await fetch_sina_kline("sh000001", 300)
        if kl:
            closes = [r["close"] for r in kl]
            last_day = kl[-1]["day"]
    except Exception:
        closes = None
    # 兜底：腾讯（Cloudflare 边缘可达，已用于 is_tx_today / 量能缓存）
    if not closes:
        try:
            tx_url = ("https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/"
                      "get?param=sh000001,day,,,300,qfq")
            tx_resp = await asyncio.wait_for(
                http_fetch(tx_url, headers={"User-Agent": UA, "Referer": "https://finance.qq.com"}), timeout=12)
            tx_j = json.loads(await tx_resp.text())
            tx_kl = (tx_j.get("data") or {}).get("sh000001", {}).get("qfqday") \
                    or (tx_j.get("data") or {}).get("sh000001", {}).get("day") or []
            # 腾讯日K数组: [日期, 开, 收, 高, 低, 量, ...] → 收盘价在下标 [2]
            tx_closes = [float(k[2]) for k in tx_kl if len(k) >= 3 and k[2] not in (None, "")]
            if tx_closes:
                closes = tx_closes
                last_day = tx_kl[-1][0] if tx_kl else None
        except Exception:
            closes = None
    if not closes:
        return None
    # 当日补点：日K末根滞后约1天时，用新浪实时 spot 补当日一根（腾讯盘中已含当日，通常跳过）
    today = beijing_now().strftime("%Y-%m-%d")
    if last_day and today > last_day:
        try:
            _, spot = await _fetch_one_index("上证指数", "1.000001", "A")
            if spot.get("price") is not None:
                closes.append(spot["price"])
        except Exception:
            pass
    last20 = closes[-20:]
    dd = max_drawdown(last20)
    ret20 = last20[-1] / last20[0] - 1
    ma20 = sum(last20) / len(last20)
    above = (closes[-1] - ma20) / ma20
    vols = roll_vol(closes, 20)
    cur = vols[-1]
    win = vols[-60:]   # 与 xiaoxu-fear 默认 vol_window=60 对齐（原 260 会造成波动率分量不一致）
    vol_pct = sum(1 for v in win if v <= cur) / len(win) if win else 0.5
    return {"drawdown": dd, "ret20": ret20, "above_ma20": above, "vol_pct": vol_pct}


def _shift_date(d, days):
    y, m, dd = map(int, str(d).split("-"))
    return (datetime(y, m, dd) - timedelta(days=days)).strftime("%Y-%m-%d")


async def fetch_sh_volume_mult(env):
    """冰点 D4 放量倍数 = 当日上证成交额(元) / 近20日均成交额(元)，对齐 xiaoxu-fear。

    【单位修正】旧实现分母基准取腾讯 qfqkline 的 k[5]（成交量·手），分子取东财 f6
    （成交额·元）→ 成交量÷成交额 单位错配，0.32 毫无意义。本版分子分母统一用成交额(元)。

    数据源与单位（均已实测核对）：
      - 今日额：腾讯 qfqkline 当日 bar 的 k[8]（成交额·万元）×1e4 = 元。
        （注：腾讯 qfqkline 的【历史】bar 被前复权缩放约 2.25 倍，不可用于历史基准；
         但【当日/近期】bar 与东财口径完全一致，故今日额取此处，UTF-8 JSON 无 GBK 问题）
      - 历史基准：东财指数日K f57（成交额·元，全日期一致），缺失时回退到 KV 中已预置的
        正确缓存（首次部署由 xiaoxu-fear 校准值初始化），绝不回退到腾讯被缩放的历史 bar。
    KV 缓存结构：dict{日期: 成交额元}，旧版 list(成交量·手) 或非 dict 即丢弃重建。
    """
    today = beijing_now().strftime("%Y-%m-%d")

    # 1. 腾讯 qfqkline：取当日额（k[8]=成交额·万元 ×1e4=元）
    today_amt = None
    try:
        tx_url = ("https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/"
                  "get?param=sh000001,day,,,5,qfq")
        tx_resp = await asyncio.wait_for(
            http_fetch(tx_url, headers={"User-Agent": UA, "Referer": "https://finance.qq.com"}), timeout=12)
        tx_j = json.loads(await tx_resp.text())
        tx_kl = (tx_j.get("data") or {}).get("sh000001", {}).get("qfqday") \
                or (tx_j.get("data") or {}).get("sh000001", {}).get("day") or []
        for k in reversed(tx_kl):
            if len(k) >= 9 and k[0] and k[8]:
                try:
                    amt = float(k[8]) * 1e4   # 万元 → 元
                    if amt > 0:
                        today_amt = amt
                        break
                except (ValueError, IndexError, TypeError):
                    continue
    except Exception:
        today_amt = None
    if not today_amt or today_amt <= 0:
        return None

    # 2. 读写 KV 滚动缓存（dict: {日期: 成交额元}；旧 list 结构或非 dict → 丢弃重建）
    cache = {}
    try:
        raw = await env.KV.get(SH_AMT_CACHE_KV_KEY)
        if raw:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                cache = {str(k): float(v) for k, v in parsed.items()
                         if isinstance(v, (int, float)) and v > 0}
    except Exception:
        cache = {}

    # 3. 样本不足(>=20) → 用东财指数日K f57(成交额·元) 补建基准（不可用则保留已预置缓存）
    if len(cache) < 20:
        try:
            em_url = ("https://push2delay.eastmoney.com/api/qt/stock/kline?secid=1.000001"
                      "&fields1=f1,f2,f3&fields2=f51,f52,f53,f54,f55,f56,f57,f58"
                      "&klt=101&fqt=0&end=20500101&lmt=45")
            em_resp = await asyncio.wait_for(
                http_fetch(em_url, headers={"User-Agent": UA,
                                       "Referer": "https://quote.eastmoney.com/"}), timeout=12)
            em_j = json.loads(await em_resp.text())
            em_kl = (em_j.get("data") or {}).get("klines") or []
            for s in em_kl:
                p = str(s).split(",")
                if len(p) >= 7 and p[0]:
                    try:
                        amt = float(p[6])   # f57 = 成交额(元)
                        if amt > 0:
                            cache.setdefault(str(p[0])[:10], amt)
                    except (ValueError, IndexError):
                        continue
        except Exception:
            pass   # 东财不可达：依赖已预置的正确缓存，或后续逐日滚动自愈

    # 4. 滚动写入今日额 + 清理 30 天前旧值
    cache[today] = today_amt
    cutoff = _shift_date(today, 30)
    cache = {d: a for d, a in cache.items() if d >= cutoff}
    try:
        await env.KV.put(SH_AMT_CACHE_KV_KEY, json.dumps(cache))
    except Exception:
        pass

    # 5. 放量倍数 = 今日额 / 近20日均额（窗口含今日，与 xiaoxu-fear valid[-20:] 一致）
    ordered = sorted((d, a) for d, a in cache.items())
    if len(ordered) < 2:
        return None
    window = [a for _, a in ordered[-20:]]
    mean_amt = sum(window) / len(window)
    return round(today_amt / mean_amt, 2) if mean_amt else None


async def _fetch_breadth_page(pn):
    url = (f"https://push2delay.eastmoney.com/api/qt/clist/get?pn={pn}&pz=100&po=1&np=1&"
           f"fltt=2&invt=2&fid=f3&fs={EM_FS_ALL}&fields=f12,f3")
    d = jload(await http_get(url))
    return (d.get("data") or {}).get("diff", []) or []


def _parse_legu_breadth(html):
    """从 legu 市场活跃度表格提取涨跌家数（纯正则，Pyodide 无 pandas/bs4）。
    表格结构：<td>上涨</td><td class="color-red">1733</td><td>下跌</td><td>3333</td>..."""
    pairs = re.findall(r'<td>([^<]+?)</td>\s*<td[^>]*>([\d.]+)</td>', html)
    d = {k.strip(): v for k, v in pairs}
    def g(name, default=0):
        try:
            return int(float(d.get(name, default)))
        except Exception:
            return default
    up = g("上涨"); down = g("下跌"); flat = g("平盘")
    lu = g("涨停"); ld = g("跌停")
    # legu 不提供「股票总数」字段 → 用 up+down+flat，与 xiaoxu-fear 真实口径一致
    total = g("股票总数", 0) or (up + down + flat)
    return {"up": up, "down": down, "flat": flat, "limit_up": lu,
            "limit_down": ld, "total": total, "_breadth_source": "legu"}


async def fetch_breadth_legu():
    """legu 单请求拿全市场广度（主源，与 xiaoxu-fear 同源）。失败返回 None。"""
    try:
        html = await http_get(LEGU_URL, ref="https://www.legulegu.com/")
        try:
            err = json.loads(html)
            if isinstance(err, dict) and "_error" in err:
                return None
        except Exception:
            pass
        b = _parse_legu_breadth(html)
        if b.get("up", 0) > 0 and b.get("down", 0) > 0:
            return b
        return None
    except Exception:
        return None


async def fetch_breadth():
    """盘面广度：legu 单请求（全市场）为主，失败回退东财分页兜底。"""
    legu = await fetch_breadth_legu()
    if legu:
        return legu
    return await _fetch_breadth_em()


async def _fetch_breadth_em():
    """（兜底）东财 clist 分页拉全市场。边缘 IP 后续分页可能返回空（覆盖不全）。"""
    up = down = lu = ld = total = 0
    pages = list(range(1, 80))
    BATCH = 10
    for i in range(0, len(pages), BATCH):
        batch = pages[i:i + BATCH]
        results = await asyncio.gather(*[_fetch_breadth_page(pn) for pn in batch])
        for diff in results:
            if not diff:
                return {"up": up, "down": down, "limit_up": lu,
                        "limit_down": ld, "total": total, "_breadth_source": "em_fallback"}
            total += len(diff)
            for x in diff:
                pct = x.get("f3")
                if not isinstance(pct, (int, float)):
                    continue
                if pct > 0:
                    up += 1
                elif pct < 0:
                    down += 1
                if pct >= 9.8:
                    lu += 1
                if pct <= -9.8:
                    ld += 1
    return {"up": up, "down": down, "limit_up": lu, "limit_down": ld,
            "total": total, "_breadth_source": "em_fallback"}


async def fetch_fund_flow():
    """东财 ulist（fltt=0！fltt=2 会把 f62/f84 清零）取沪深两市主力/散户净流入占比。"""
    url = (f"https://push2delay.eastmoney.com/api/qt/ulist.np/get?fltt=0&"
           f"secids=1.000001,0.399001&fields=f62,f84,f6&"
           f"ut=b2884a393a59ad64002292a3e90d46a5")
    d = jload(await http_get(url))
    diff = (d.get("data") or {}).get("diff", []) or []
    s_main = s_retail = s_amt = 0.0
    for it in diff:
        amt = it.get("f6") or 0
        s_main += (it.get("f62") or 0)
        s_retail += (it.get("f84") or 0)
        s_amt += amt
    if s_amt:
        return {"main_net": s_main / s_amt, "retail_net": s_retail / s_amt}
    return {"main_net": None, "retail_net": None}


async def fetch_idx_chg():
    """上证/创业板当日涨跌幅（冰点 D2）。"""
    out = {}
    for name, secid in [("上证指数", "1.000001"), ("创业板指", "0.399006")]:
        url = (f"https://push2delay.eastmoney.com/api/qt/stock/get?fltt=2&invt=2&"
               f"secid={secid}&fields=f58,f170")
        d = jload(await http_get(url))
        q = d.get("data") or {}
        out[name] = q.get("f170")
    return out


async def fetch_etf_down_ratio():
    """核心 ETF 篮子跌幅<=-2.5% 占比（冰点 D2）。"""
    def secid_of(code):
        # 沪市 ETF: 5 开头（51/52/55/56/58）或 1 开头非 15（510/512/513/515/518）
        # 深市 ETF: 15 开头（159xxx）
        if code[0] == "5":
            return "1." + code
        if code[0] == "1":
            return ("0." if code[1] == "5" else "1.") + code
        return "1." + code
    secids = ",".join(secid_of(c) for c in ETF_BASKET)
    url = (f"https://push2delay.eastmoney.com/api/qt/ulist.np/get?fltt=2&"
           f"secids={secids}&fields=f12,f14,f3")
    d = jload(await http_get(url))
    items = (d.get("data") or {}).get("diff", []) or []
    if not items:
        return {"ratio": None, "n": 0, "down": 0}
    n_down = sum(1 for x in items if (x.get("f3") or 0) <= -2.5)
    return {"ratio": n_down / len(items), "n": len(items), "down": n_down}


async def fetch_valuation():
    """蛋卷估值目录：一次拿全指数 PE/PB/分位/股息率。"""
    url = "https://danjuanfunds.com/djapi/index_eva/dj"
    d = jload(await http_get(url, ref="https://danjuanfunds.com/"))
    items = (d.get("data") or {}).get("items", []) or []
    by_code = {it.get("index_code"): it for it in items}
    out = {}
    for name, code in VALUATION_CODES.items():
        it = by_code.get(code) or {}
        out[name] = {
            "name": name,
            "pe": it.get("pe"), "pb": it.get("pb"),
            "pe_pct": it.get("pe_percentile"), "pb_pct": it.get("pb_percentile"),
            "yield": it.get("yeild"), "date": it.get("date"),
        }
    return out


async def fetch_us_fear():
    """美股恐慌指数：CNN Fear & Greed + VIX + VXN（三路并发，失败降级）。"""
    async def _vix(sym):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/%5E{sym}?interval=1d&range=1d"
        try:
            r = jload(await http_get(url, ref="https://finance.yahoo.com"))
            meta = (r.get("chart") or {}).get("result", [{}])[0].get("meta", {})
            return {"price": meta.get("regularMarketPrice"), "prev_close": meta.get("previousClose")}
        except Exception:
            return None
    async def _fng():
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        try:
            r = jload(await http_get(url, ref="https://www.cnn.com"))
        except Exception:
            return None
        try:
            fng = r.get("fear_and_greed", {})
            return {"score": fng.get("score"), "rating": fng.get("rating"), "timestamp": fng.get("timestamp")}
        except Exception:
            return None
    vix, vxn, fng = await asyncio.gather(_vix("VIX"), _vix("VXN"), _fng())
    return {"vix": vix, "vxn": vxn, "fear_greed": fng}


# ============================ 计算（精确移植源逻辑） ============================
def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def _f(v, default=0.0):
    """强制转为 float，None / 非数值统一降级为 default（边缘环境部分源返回 None 时防崩）。"""
    return v if isinstance(v, (int, float)) else default


XXFI_WEIGHTS = {
    "fear": {"drawdown": 0.30, "breadth": 0.25, "limitdown": 0.20, "vol": 0.25},
    "greed": {"momentum": 0.25, "limitup": 0.15, "retailin": 0.20,
              "overbought": 0.20, "divergence": 0.20},
}
DIVERGENCE_K = 200


def compute_xxfi(d):
    dd = abs(_f(d.get("drawdown")))
    f_drawdown = clamp(dd * 500)
    down, up = _f(d.get("down", 1)), max(1, _f(d.get("up", 1)))
    breadth = down / up
    f_breadth = clamp((breadth - 0.5) * 100)
    lu = max(1, _f(d.get("limit_up", 1)))
    ld = _f(d.get("limit_down", 0))
    f_limitdown = clamp((ld / lu) * 50)
    vol_pct = _f(d.get("vol_pct", 0.5))
    f_vol = clamp(vol_pct * 100)
    fear = (XXFI_WEIGHTS["fear"]["drawdown"] * f_drawdown +
            XXFI_WEIGHTS["fear"]["breadth"] * f_breadth +
            XXFI_WEIGHTS["fear"]["limitdown"] * f_limitdown +
            XXFI_WEIGHTS["fear"]["vol"] * f_vol)

    ret20 = _f(d.get("ret20"))
    g_momentum = clamp(ret20 * 500)
    g_limitup = clamp((lu / max(1, ld)) * 50)
    retail_net = _f(d.get("retail_net"))
    g_retailin = clamp(retail_net * 200)
    above = _f(d.get("above_ma20"))
    g_overbought = clamp(above * 500)

    main_net = d.get("main_net", None)
    fund_ok = isinstance(main_net, (int, float)) and main_net is not None \
        and isinstance(retail_net, (int, float))
    if fund_ok:
        main_f, ret_f = float(main_net), float(retail_net)
        divergence = main_f - ret_f
        g_divergence = clamp(50 - divergence * DIVERGENCE_K)
        if abs(divergence) < 0.005:
            div_state = "同向 / 无显著背离"
        elif main_f < 0 and ret_f > 0:
            div_state = "顶部出货（散户追高·主力派发）"
        elif main_f > 0 and ret_f < 0:
            div_state = "底部吸筹（散户割肉·主力进场）"
        else:
            div_state = "同向（主散同方向）"
    else:
        divergence = None
        g_divergence = 50.0
        div_state = "无数据（资金流降级）"

    greed = (XXFI_WEIGHTS["greed"]["momentum"] * g_momentum +
             XXFI_WEIGHTS["greed"]["limitup"] * g_limitup +
             XXFI_WEIGHTS["greed"]["retailin"] * g_retailin +
             XXFI_WEIGHTS["greed"]["overbought"] * g_overbought +
             XXFI_WEIGHTS["greed"]["divergence"] * g_divergence)

    xxfi = fear
    if xxfi >= 75:
        extreme, contrarian = "FEAR", "BUY"
    elif xxfi >= 60:
        extreme, contrarian = "FEAR", "ACCUMULATE"
    elif xxfi >= 40:
        extreme, contrarian = "NEUTRAL", "HOLD"
    elif xxfi >= 25:
        extreme, contrarian = "GREED", "REDUCE"
    else:
        extreme, contrarian = "GREED", "SELL"

    if xxfi >= 75:
        level, advice = ("极度恐惧（小旭式恐慌割肉区）",
                         "历史校准：小旭在连跌后恐慌割肉，卖后多现 +9%~+24% 反弹。→ 反向强烈看多，分批低吸。")
    elif xxfi >= 60:
        level, advice = ("恐惧（偏谨慎，她倾向割肉）",
                         "市场情绪偏弱，但接近她‘卖飞’区。→ 逢低吸纳，避免跟风杀跌。")
    elif xxfi >= 40:
        level, advice = ("中性", "恐惧与贪婪均衡，无明显反向极值。→ 按自身策略持有，不依赖本指标。")
    elif xxfi >= 25:
        level, advice = ("偏贪婪（情绪偏热，她倾向追高）",
                         "市场恐惧偏低、热度偏高。→ 逢高减仓，不追涨；小旭常在连续大涨后追高买在山顶。")
    else:
        level, advice = ("极度贪婪（小旭式追涨山顶区）",
                         "历史校准：小旭追高买在山顶后多现 -12%~-21% 回落。→ 反向强烈看空，减仓避险。")

    if divergence is not None and abs(divergence) >= 0.005:
        aligned = (divergence < 0 and contrarian in ("REDUCE", "SELL")) or \
                  (divergence > 0 and contrarian in ("BUY", "ACCUMULATE"))
        tag = "背离确认" if aligned else "背离提示"
        advice = advice + f"　[{tag}·{div_state}]"

    return {
        "XXFI": round(xxfi, 1),
        "GreedIndex": round(greed, 1),
        "extreme": extreme,
        "contrarian_signal": contrarian,
        "level": level,
        "advice": advice,
        "divergence": round(divergence, 4) if divergence is not None else None,
        "divergence_state": div_state,
        "components": {
            "fear": {"drawdown": round(f_drawdown, 1), "breadth": round(f_breadth, 1),
                     "limitdown": round(f_limitdown, 1), "vol": round(f_vol, 1)},
            "greed": {"momentum": round(g_momentum, 1), "limitup": round(g_limitup, 1),
                      "retailin": round(g_retailin, 1), "overbought": round(g_overbought, 1),
                      "divergence": round(g_divergence, 1)},
        },
    }


BINGDIAN_TH = {
    "D1_down_count": 4000, "D1_down_ratio": 0.85,
    "D2_sh": -2.0, "D2_cyb": -2.5, "D2_etf_ratio": 0.60,
    "D3_limit_down": 50, "D3_ld_lu_ratio": 3.0, "D4_volume_mult": 1.3,
}


def _ok(v):
    return v is not None and v != "暂未获取"


def compute_bingdian(d):
    down, total, ratio = d.get("down"), d.get("total"), d.get("down_ratio")
    d1_ok = _ok(down) and _ok(total) and _ok(ratio)
    d1 = d1_ok and int(down) >= BINGDIAN_TH["D1_down_count"] and float(ratio) >= BINGDIAN_TH["D1_down_ratio"]

    sh, cyb, etf = d.get("sh_chg"), d.get("cyb_chg"), d.get("etf_down_ratio")
    d2_ok = _ok(sh) and _ok(cyb) and _ok(etf)
    d2 = d2_ok and float(sh) <= BINGDIAN_TH["D2_sh"] and float(cyb) <= BINGDIAN_TH["D2_cyb"] \
        and float(etf) >= BINGDIAN_TH["D2_etf_ratio"]

    ld, lu, ldl = d.get("limit_down"), d.get("limit_up"), d.get("ld_lu_ratio")
    d3_ok = _ok(ld) and _ok(lu) and _ok(ldl)
    d3 = d3_ok and int(ld) >= BINGDIAN_TH["D3_limit_down"] and float(ldl) >= BINGDIAN_TH["D3_ld_lu_ratio"]

    vm = d.get("volume_mult")
    d4_ok = _ok(vm)
    d4 = d4_ok and float(vm) >= BINGDIAN_TH["D4_volume_mult"]

    verdict = bool(d1 and d2 and d3 and d4)
    dims = [
        {"key": "D1", "name": "下跌广度",
         "value": f"下跌 {int(down)}/{int(total)}（{float(ratio)*100:.1f}%）" if d1_ok else "暂未获取",
         "threshold": f"下跌≥{BINGDIAN_TH['D1_down_count']} 且 占比≥{BINGDIAN_TH['D1_down_ratio']*100:.0f}%",
         "pass": (d1 if d1_ok else None)},
        {"key": "D2", "name": "指数/ETF跌幅",
         "value": f"上证 {float(sh):.2f}% 创业 {float(cyb):.2f}% · ETF跌 {float(etf)*100:.0f}%" if d2_ok else "暂未获取",
         "threshold": f"上证≤{BINGDIAN_TH['D2_sh']}% 创业≤{BINGDIAN_TH['D2_cyb']}% ETF跌≥{BINGDIAN_TH['D2_etf_ratio']*100:.0f}%",
         "pass": (d2 if d2_ok else None)},
        {"key": "D3", "name": "跌停数量",
         "value": f"跌停 {int(ld)}/涨停 {int(lu)}（比 {float(ldl):.1f}）" if d3_ok else "暂未获取",
         "threshold": f"跌停≥{BINGDIAN_TH['D3_limit_down']} 且 比≥{BINGDIAN_TH['D3_ld_lu_ratio']:.0f}",
         "pass": (d3 if d3_ok else None)},
        {"key": "D4", "name": "放量恐慌",
         "value": f"{float(vm):.2f} 倍" if d4_ok else "暂未获取",
         "threshold": f"放量倍数≥{BINGDIAN_TH['D4_volume_mult']}",
         "pass": (d4 if d4_ok else None)},
    ]
    return {
        "verdict": verdict,
        "verdict_text": "冰点" if verdict else "非冰点",
        "verdict_emoji": "🔥" if verdict else "🧊",
        "verdict_full": ("🔥 冰点触发 · 极端恐慌带血筹码" if verdict else "🧊 未至冰点 · 纪律不出手"),
        "dimensions": dims,
        "missing": [x["key"] for x in dims if x["pass"] is None],
    }


# ============================ 编排 ============================
def beijing_now():
    return datetime.now(timezone.utc) + timedelta(hours=8)


async def _cron_mark(env, stage, **kw):
    """把定时触发的每一步状态写入 KV，避免静默失败（手机端 /api/cron_diag 可读）。"""
    try:
        rec = {"stage": stage, "at": beijing_now().strftime("%Y-%m-%d %H:%M:%S"), **kw}
        await env.KV.put(CRON_DIAG_KV, json.dumps(rec, ensure_ascii=False, default=str))
    except Exception:
        pass


async def is_tx_today():
    """通过 腾讯日K 查今天是否有数据来判断是否为交易日（与 xiaoxu-fear 同源）。
    交易日会有今日的日K数据（即使盘中），非交易日无数据。腾讯日K任何时段可达。"""
    try:
        url = ("https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/"
               "get?param=sh000001,day,,,2,qfq")
        resp = await asyncio.wait_for(
            http_fetch(url, headers={"User-Agent": UA, "Referer": "https://finance.qq.com"}), timeout=12)
        j = json.loads(await resp.text())
        kl = (j.get("data") or {}).get("sh000001", {}).get("qfqday") \
             or (j.get("data") or {}).get("sh000001", {}).get("day") or []
        today_str = beijing_now().strftime("%Y-%m-%d")
        for k in kl:
            if len(k) >= 1 and k[0] == today_str:
                return True
        return False
    except Exception:
        return True  # 网络失败时默认放行，不阻断交易


async def build_snapshot(env):
    """抓取全部源 + 计算，返回快照 dict。任一源失败优雅降级。"""
    snap = {"generated_at": beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "tz": "Asia/Shanghai", "degraded": []}

    # 10 路抓取彼此独立 → 一次性并发，总耗时=最慢一路（广度），大幅压缩窗口
    (indices, commodities, hs300, vol_mult, breadth, fund,
     idx_chg, etf, valuation, us_fear) = await asyncio.gather(
        fetch_index_quotes(), fetch_commodities(), fetch_hs300_deriv(),
        fetch_sh_volume_mult(env), fetch_breadth(), fetch_fund_flow(),
        fetch_idx_chg(), fetch_etf_down_ratio(), fetch_valuation(),
        fetch_us_fear())

    snap["indices"] = indices
    snap["commodities"] = commodities
    snap["valuation"] = valuation

    # ---- XXFI 输入装配 ----
    xxfi_in = {
        "drawdown": (hs300 or {}).get("drawdown", 0.0),
        "ret20": (hs300 or {}).get("ret20", 0.0),
        "above_ma20": (hs300 or {}).get("above_ma20", 0.0),
        "vol_pct": (hs300 or {}).get("vol_pct", 0.5),
        "up": breadth.get("up", 1), "down": breadth.get("down", 1),
        "limit_up": breadth.get("limit_up", 1), "limit_down": breadth.get("limit_down", 0),
        "retail_net": fund.get("retail_net", 0.0), "main_net": fund.get("main_net"),
    }
    snap["xxfi"] = compute_xxfi(xxfi_in)
    snap["xxfi_inputs"] = {k: (round(v, 6) if isinstance(v, float) else v)
                           for k, v in xxfi_in.items()}

    # ---- 冰点 输入装配 ----
    total = breadth.get("total", 1) or 1
    bd_in = {
        "down": breadth.get("down", 0), "total": total,
        "down_ratio": breadth.get("down", 0) / total,
        "sh_chg": idx_chg.get("上证指数"), "cyb_chg": idx_chg.get("创业板指"),
        "etf_down_ratio": etf.get("ratio"),
        "limit_down": breadth.get("limit_down", 0), "limit_up": breadth.get("limit_up", 1),
        "ld_lu_ratio": breadth.get("limit_down", 0) / max(1, breadth.get("limit_up", 1)),
        "volume_mult": vol_mult,
    }
    snap["bingdian"] = compute_bingdian(bd_in)
    snap["breadth"] = breadth
    snap["breadth_source"] = breadth.get("_breadth_source", "unknown")
    snap["etf_down"] = etf
    snap["us_fear"] = us_fear

    # 降级标记
    if hs300 is None:
        snap["degraded"].append("沪深300日K(波动率/回撤)")
    if fund.get("main_net") is None:
        snap["degraded"].append("主力/散户资金流")
    if etf.get("ratio") is None:
        snap["degraded"].append("ETF篮子跌幅")
    if vol_mult is None:
        snap["degraded"].append("上证量能(放量倍数)")
    if not valuation:
        snap["degraded"].append("估值水位")
    return snap


async def publish_to_github(snap, env):
    """把快照推到 GitHub Pages 仓库（best-effort，失败不阻断主流程）。
    需 env.GITHUB_TOKEN（Cloudflare secret）。推到 docs/data.json，
    dashboard（github.io）读 ./data.json 实现国内直连。"""
    tok = getattr(env, "GITHUB_TOKEN", None)
    if not tok:
        # 诊断留痕：secret 为空/未绑定 → 从不推送（静默失败根因之一）。
        # 写入 _gh_diag，便于 /api/cron_diag 一眼定位（与 _cron_diag 同源）。
        try:
            await env.KV.put("_gh_diag", json.dumps(
                {"stage": "no_token",
                 "at": beijing_now().strftime("%Y-%m-%d %H:%M:%S")},
                ensure_ascii=False))
        except Exception:
            pass
        return
    try:
        content = base64.b64encode(
            json.dumps(snap, ensure_ascii=False, default=str).encode("utf-8")
        ).decode("ascii")
        url = f"{GH_API}/repos/{GH_REPO}/contents/{GH_DATA_PATH}"
        headers = {"Authorization": f"Bearer {tok}",
                   "Accept": "application/vnd.github+json",
                   "User-Agent": UA, "Content-Type": "application/json"}
        # 取现有 sha（更新需要）
        sha = None
        try:
            r = await asyncio.wait_for(http_fetch(url, headers=headers), timeout=12)
            j = json.loads(await r.text())
            sha = j.get("sha")
        except Exception:
            sha = None
        body = {"message": f"data update {snap.get('generated_at', '')}",
                "content": content}
        if sha:
            body["sha"] = sha
        await asyncio.wait_for(
            http_fetch(url, method="PUT", headers=headers, body=json.dumps(body)),
            timeout=12)
        # 成功留痕：便于 /api/cron_diag 确认自动化推送已生效
        try:
            await env.KV.put("_gh_diag", json.dumps(
                {"stage": "ok",
                 "at": beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
                 "generated_at": snap.get("generated_at", "")},
                ensure_ascii=False))
        except Exception:
            pass
    except Exception as e:
        snap.setdefault("degraded", [])
        if "github" not in snap["degraded"]:
            snap["degraded"].append("github_push")
        try:
            await env.KV.put("_gh_diag", json.dumps(
                {"stage": "error", "err": f"{type(e).__name__}: {e}",
                 "at": beijing_now().strftime("%Y-%m-%d %H:%M:%S")},
                ensure_ascii=False, default=str))
        except Exception:
            pass


async def _gh_put(path, content, message, env):
    """向 GitHub 仓库 PUT 一个文件（公用）。"""
    tok = getattr(env, "GITHUB_TOKEN", None)
    if not tok:
        return
    try:
        url = f"{GH_API}/repos/{GH_REPO}/contents/{path}"
        headers = {"Authorization": f"Bearer {tok}",
                   "Accept": "application/vnd.github+json",
                   "User-Agent": UA, "Content-Type": "application/json"}
        sha = None
        try:
            r = await asyncio.wait_for(http_fetch(url, headers=headers), timeout=12)
            j = json.loads(await r.text())
            sha = j.get("sha")
        except Exception:
            sha = None
        body = {"message": message, "content": content}
        if sha:
            body["sha"] = sha
        await asyncio.wait_for(
            http_fetch(url, method="PUT", headers=headers, body=json.dumps(body)),
            timeout=12)
    except Exception:
        pass


async def publish_index_html(env):
    """把 public/index.html 原样推到 GitHub Pages。

    源码 public/index.html 已内置 GH 模式适配（IS_GH 运行时判断 hostname，
    github.io 下读 ./data.json、隐藏刷新按钮），无需再做字符串注入——
    旧逻辑二次注入 IS_GH 会导致整页 JS 因重复声明而崩溃（空白页）。
    """
    tok = getattr(env, "GITHUB_TOKEN", None)
    if not tok:
        return
    try:
        req = WorkersRequest("https://dummy/")
        resp = await env.ASSETS.fetch(req)
        html = await resp.text()
        content = base64.b64encode(html.encode("utf-8")).decode("ascii")
        await _gh_put("docs/index.html", content,
                      f"index update {beijing_now().strftime('%Y-%m-%d %H:%M:%S')}", env)
        # 同步推送 README（base64 预编码，本次会话已更新内容）
        try:
            await _gh_put("README.md", README_B64,
                         f"readme update {beijing_now().strftime('%Y-%m-%d %H:%M:%S')}", env)
        except Exception:
            pass
    except Exception:
        pass


async def refresh_and_store(env):
    try:
        snap = await build_snapshot(env)
    except Exception as e:
        import traceback
        snap = {
            "generated_at": beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "tz": "Asia/Shanghai",
            "degraded": ["严重异常"],
            "_error": f"{type(e).__name__}: {e}",
            "_trace": traceback.format_exc()[-2500:],
        }
    # 先写主数据源 KV（面板 /api/data 直接读这里），确保即使 GitHub 推送偶发失败，KV 仍有最新值
    try:
        await env.KV.put(KV_KEY, json.dumps(snap, ensure_ascii=False, default=str))
    except Exception as e:
        snap["_kv_error"] = str(e)
    # 再推 GitHub Pages（只读镜像，失败仅滞后，不致命）
    await publish_to_github(snap, env)   # best-effort，失败会追加 degraded
    await publish_index_html(env)        # best-effort 推 GH Pages 版 dashboard
    return snap


# ============================ 入口 ============================
def _path_of(request):
    """从 Request 取出 path（Cloudflare Python Worker 中 request.url 为字符串）。"""
    raw = str(request.url) if hasattr(request, "url") else str(request)
    no_q = raw.split("?", 1)[0]
    dpos = no_q.find("//")
    if dpos != -1:
        idx = no_q.find("/", dpos + 2)
        return no_q[idx:] if idx != -1 else "/"
    return no_q or "/"


async def _cron_run(env):
    """定时刷新的实际逻辑（env 由 scheduled 直接传入，必有效）。

    仅以 is_tx_today() 判定是否为交易日（窗口判断已按简化移除）；
    非交易日 → 留痕 skipped 并返回；否则刷新 KV 并留痕 dispatched；异常留痕 error。
    """
    is_tx = await is_tx_today()
    try:
        await _cron_mark(env, "enter", is_tx=is_tx)
    except Exception:
        pass
    if not is_tx:
        try:
            await _cron_mark(env, "skipped", is_tx=is_tx)
        except Exception:
            pass
        return
    try:
        await refresh_and_store(env)
        await _cron_mark(env, "dispatched")
        print("CRON-OK dispatched")
    except Exception as e:
        import traceback
        tb = traceback.format_exc()[-1500:]
        print("CRON-ERROR", repr(e), tb)
        try:
            await _cron_mark(env, "error",
                            err=f"{type(e).__name__}: {e}", tb=tb)
        except Exception:
            pass


class Default(WorkerEntrypoint):
    """market-live 入口：HTTP 路由 + Cron 定时刷新（京时间交易时段）。

    采用 Cloudflare Python Workers 规范：Default(WorkerEntrypoint) 类。
    fetch 与 scheduled 一律通过 self.env 取绑定（KV / GITHUB_TOKEN / ASSETS）；
    scheduled 另以 (self, controller, env, ctx) 4 个位置参数接收运行时调用，
    但位置参数里的 env 不可靠，已统一改从 self.env 取真实绑定。
    """

    async def fetch(self, request):
        try:
            path = _path_of(request)

            if path == "/api/data":
                raw = await self.env.KV.get(KV_KEY)
                if not raw:
                    # KV 为空（首访 / Cron 尚未触发）：同步构建并返回，保证首次访问即可拿到数据
                    snap = await refresh_and_store(self.env)
                    return _json(json.dumps(snap, ensure_ascii=False, default=str))
                return _json(raw)

            if path == "/api/refresh":
                # 手动刷新：同步构建并返回最新快照（供面板刷新按钮使用）
                snap = await refresh_and_store(self.env)
                return _json(json.dumps(snap, ensure_ascii=False, default=str))

            if path == "/api/cron_run":
                # 供 scheduled 自调用：完整交易日判断 + 刷新 + 诊断留痕（拥有完整 env）
                await _cron_run(self.env)
                return _json(json.dumps({"status": "cron_run_done",
                                         "tz": "Asia/Shanghai",
                                         "at": beijing_now().strftime("%Y-%m-%d %H:%M:%S")},
                                        ensure_ascii=False))

            if path == "/api/cron_diag":
                # 只读诊断：返回最近一次定时触发状态（成功/跳过/崩溃）+ 主快照时间
                diag = (await self.env.KV.get(CRON_DIAG_KV)) or "{}"
                snap_raw = (await self.env.KV.get(KV_KEY)) or "{}"
                try:
                    snap_ts = json.loads(snap_raw).get("generated_at")
                except Exception:
                    snap_ts = None
                return _json(json.dumps({"cron_diag": json.loads(diag),
                                         "last_snapshot_at": snap_ts}, ensure_ascii=False))

            # 静态面板
            try:
                return await self.env.ASSETS.fetch(request)
            except Exception:
                return _html(FALLBACK_HTML, 200)
        except Exception as e:
            import traceback
            msg = "HANDLER_ERROR: %s\n%s" % (e, traceback.format_exc()[-2500:])
            try:
                await self.env.KV.put("_diag", msg)
            except Exception:
                pass
            return Response(msg, status=500,
                            headers={"content-type": "text/plain; charset=utf-8"})

    async def scheduled(self, controller, env=None, ctx=None):
        """Cron 定时刷新。

        ⚠️ 运行时按 (self, controller, env, ctx) 传 4 个位置参数，故必须全部接收，
        否则会抛 "takes N positional arguments but 4 were given"（此前 846c4c90 根因）。

        但实测：scheduled 位置参数里的 env 并非真实绑定对象（用它写 KV 静默失败、
        _cron_diag 始终 404、GitHub 不推送）。真正可靠的绑定来源是 self.env
        （与 fetch 一致，手动 /api/refresh 已验证可写 KV / 推 GitHub）。故此处
        优先 self.env，并在位置参数里兜底挑出真正带 .KV 的那个；两者皆无则打印
        CRON-NO-ENV 诊断，把“静默失败”变成“可见日志”。
        """
        real_env = getattr(self, "env", None)
        if real_env is None or not hasattr(real_env, "KV"):
            for cand in (env, ctx):
                if cand is not None and hasattr(cand, "KV"):
                    real_env = cand
                    break
        if real_env is None or not hasattr(real_env, "KV"):
            print("CRON-NO-ENV self.env_type=",
                  type(getattr(self, "env", None)).__name__,
                  "positional=", [(a, type(a).__name__) for a in (env, ctx)])
            return
        print("CRON-ENV-OK type=", type(real_env).__name__,
              "has_KV=", hasattr(real_env, "KV"))
        try:
            await _cron_run(real_env)
            print("CRON-OK scheduled dispatched")
        except Exception as e:
            import traceback
            tb = traceback.format_exc()[-1500:]
            print("CRON-ERROR", repr(e), tb)
            try:
                await _cron_mark(real_env, "error",
                                err=f"{type(e).__name__}: {e}", tb=tb)
            except Exception:
                pass


def _json(body):
    return Response(body, headers={"content-type": "application/json; charset=utf-8",
                                   "cache-control": "no-store"})


def _html(body, status=200):
    return Response(body, status=status,
                    headers={"content-type": "text/html; charset=utf-8",
                             "cache-control": "no-store"})


# 兜底 HTML（当未配置 ASSETS 或静态缺失时使用，保证可访问）
FALLBACK_HTML = """<!doctype html><html lang=zh><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>market-live</title><body style="font-family:sans-serif;padding:24px">
<h2>market-live 实时数据层</h2>
<p>静态面板未加载。请访问 <code>/api/data</code> 获取 JSON，或确认 public/index.html 已部署。</p>
<p><button onclick="location.reload()">刷新</button></p></body></html>"""
