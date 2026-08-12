# -*- coding: utf-8 -*-
"""
market-live · Cloudflare Python Worker
=======================================
实时数据层：自算「小旭恐惧指数(XXFI)」+「A股冰点」，并抓取
A/港/美/全球指数、大宗商品、汇率、估值水位，写入 KV，供前端面板读取。
触发规则详见 wrangler.toml（Cron: 北京时间周一至周五 9:45–16:15 每 30 分钟，14 次/交易日；Cloudflare 按 UTC 评估，故 wrangler.toml 用 3 条 mon-fri 表达式 +8 小时补偿）。
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

# ---------------- 年内涨跌幅（YTD）基数 ----------------
YTD_BASE_KV_PREFIX = "ytd_base_"
# A股(新浪日K) / 港股+全球+汇率(东财日K) / 美股(雅虎1y)
YTD_SINA_KL = {
    "上证指数": "sh000001", "深证成指": "sz399001", "创业板指": "sz399006",
    "沪深300": "sh000300", "科创50": "sh000688", "北证50": "bj899050",
    "红利低波": "sh000069",   # 红利低波指数
    "30年国债ETF": "sh511130", # 30年国债ETF（有K线可算年内）
}
YTD_EM_KL = {
    "恒生指数": "100.HSI", "恒生科技": "124.HSTECH", "恒生国企指数": "100.HSCEI",
    "日经225": "100.N225", "韩国KOSPI": "100.KS11", "德国DAX": "100.GDAXI",
    "欧洲斯托克600": "100.SXXP", "法国CAC40": "100.FCHI", "英国富时100": "100.FTSE",
    "美元离岸人民币": "133.USDCNH",
}
YTD_US_YAHOO = {
    "标普500": "%5EGSPC", "纳斯达克100": "%5ENDX", "纳斯达克综合": "%5EIXIC",
    "道琼斯": "%5EDJI", "美国红利指数ETF(SCHD)": "SCHD", "半导体ETF": "SOXX",
    "标普500期货": "ES%3DF", "纳指100期货": "NQ%3DF",
}
YTD_NO_BASE = {"WTI原油", "COMEX黄金", "布伦特原油", "白银", "30年国债期货"}

def _ytd_year():
    return beijing_now().strftime("%Y")

async def fetch_sina_ytd_base(symbol):
    url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           f"CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen=320")
    d = jload(await http_get(url, ref="https://finance.sina.com.cn"))
    if not isinstance(d, list) or not d:
        return None
    y = _ytd_year()
    for r in d:
        if r.get("day", "").startswith(y):
            try: return float(r["close"])
            except (ValueError, TypeError): return None
    return None

async def fetch_em_ytd_base(secid):
    url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?"
           f"secid={secid}&fields1=f1,f2,f3&fields2=f51,f53&klt=101&fqt=0"
           f"&beg={_ytd_year()}0101&end={_ytd_year()}1231&lmt=5")
    # 东财对海外 secid 偶发 520，重试 3 次
    for i in range(3):
        try:
            d = jload(await http_get(url))
            klines = ((d.get("data") or {}).get("klines")) or []
            if klines:
                first = klines[0].split(",")
                return float(first[1])
        except Exception:
            pass
        await asyncio.sleep(i * 0.5)
    return None

async def fetch_yahoo_ytd_base(sym):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
           f"?interval=1d&range=1y")
    r = jload(await http_get(url, ref="https://finance.yahoo.com"))
    res = ((r.get("chart") or {}).get("result") or [{}])[0]
    ts = (res.get("timestamp") or [])
    cl = ((res.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    y = _ytd_year()
    for t, c in zip(ts, cl):
        import datetime as _dt
        if _dt.datetime.fromtimestamp(t, tz=_dt.timezone.utc).strftime("%Y") == y and c is not None:
            return float(c)
    return None

async def load_ytd_bases(env):
    """每次运行重新抓取年初基数（不缓存），基数一年内不变，ytd 随当前价实时变化。"""
    bases = {}
    async def _one(name, fetch_fn, arg):
        try:
            v = await fetch_fn(arg)
            if v: bases[name] = v
        except Exception:
            pass
    tasks = []
    for n, sym in YTD_SINA_KL.items():
        tasks.append(_one(n, fetch_sina_ytd_base, sym))
    for n, sec in YTD_EM_KL.items():
        tasks.append(_one(n, fetch_em_ytd_base, sec))
    for n, sym in YTD_US_YAHOO.items():
        tasks.append(_one(n, fetch_yahoo_ytd_base, sym))
    await asyncio.gather(*tasks)
    return bases


def attach_ytd(snap, bases):
    for d in (snap.get("indices") or {}).values():
        name = d.get("name")
        base = bases.get(name)
        p = d.get("price")
        d["ytd"] = round((p / base - 1) * 100, 2) if (base and p) else None
    for d in (snap.get("commodities") or {}).values():
        d["ytd"] = None
    for d in (snap.get("us_quotes") or []) or []:
        name = d.get("name")
        base = bases.get(name)
        p = d.get("price")
        d["ytd"] = round((p / base - 1) * 100, 2) if (base and p) else None
    return snap
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
    "红利低波": ("2.H30269", "A"),
    "30年国债ETF": ("1.511130", "A"),
    "恒生指数":   ("100.HSI",  "H"),
    "恒生科技":   ("124.HSTECH", "H"),
    "恒生国企指数": ("100.HSCEI", "H"),
    "日经225":    ("100.N225",  "G"),
    "韩国KOSPI":  ("100.KS11",  "G"),
    "德国DAX":    ("100.GDAXI", "G"),
    "欧洲斯托克600": ("100.SXXP", "G"),
    "法国CAC40":  ("100.FCHI",  "G"),
    "英国富时100": ("100.FTSE",  "G"),
}
# 全球/美股额外展示（已含于上表，这里仅分类）
GROUPS = {
    "A":  ["上证指数", "深证成指", "创业板指", "沪深300", "科创50", "北证50", "红利低波", "30年国债ETF", "30年国债期货"],
    "H":  ["恒生指数", "恒生科技", "恒生国企指数"],
    "G":  ["日经225", "韩国KOSPI", "德国DAX", "欧洲斯托克600", "法国CAC40", "英国富时100"],
}
# 大宗商品（新浪 hf_）
COMMODITIES = {
    "WTI原油": "hf_CL",
    "COMEX黄金": "hf_GC",
    "布伦特原油": "hf_OIL",
    "白银": "hf_SI",
}
# 内盘国债期货（新浪 nf_，与 hf_ 字段布局不同：名称在末尾，名称前末位数值=昨结算）
TREASURY_FUTURES = {
    "30年国债期货": "nf_TL0",
}
# 离岸人民币（东财）
CNH_SECID = "133.USDCNH"
# 估值（蛋卷 index_eva/dj，按 index_code 抽取）
VALUATION_CODES = {
    "上证50": "SH000016",
    "沪深300": "SH000300",
    "创业板指": "SZ399006",
    "科创50": "SH000688",
    "中概互联50": "CSIH30533",
    "恒生科技": "HKHSTECH",
    "中证白酒": "SZ399997",
    "中证银行": "SZ399986",
    "中证红利": "SH000922",
    "中证红利低波": "CSIH30269",
    "标普500": "SP500",
    "纳指100": "NDX",
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
README_B64 = "IyBtYXJrZXQtbGl2ZSDCtyDlrp7ml7bluILlnLrnnIvmnb8KCj4gKirlm73lhoXnm7Tov54qKu+8mmh0dHBzOi8vaG9tamFub24uZ2l0aHViLmlvL21hcmtldC1saXZlL++8iEdpdEh1YiBQYWdlc++8jOS4reWbveWGheWcsOebtOi/nu+8iSAgCj4gKipWUE4qKu+8mmh0dHBzOi8vbWFya2V0LWxpdmUuaG9tamFub24ud29ya2Vycy5kZXbvvIhDbG91ZGZsYXJlIFdvcmtlcu+8jOWQq+aJi+WKqOWIt+aWsOaMiemSru+8iQoK5a6e5pe25bGV56S6IEEg6IKh44CB5riv6IKh44CB576O6IKh44CB5YWo55CD5Li76KaB5oyH5pWw44CB5aSn5a6X5ZWG5ZOB44CB5rGH546H44CB5Lyw5YC85rC05L2N77yM5bm26Ieq566XKirlsI/ml63mgZDmg6fmjIfmlbDvvIhYWEZJ77yJKiog5LiOICoqQSDogqHlhrDngrkqKuWPguiAg+aMh+agh+OAggoKLS0tCgojIyDmnrbmnoQgwrcg5pWw5o2u5rWBCgpgYGAK4pSM4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSQCuKUgiAgICAgICAgICAgQ2xvdWRmbGFyZSBQeXRob24gV29ya2VyICAgICAgICAgICAgICDilIIK4pSCICAoUHlvZGlkZSDov5DooYzml7YsIENyb24g5q+PMzDliIbpkp/op6blj5EpICAgICAgICAgICAgIOKUggrilIIgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICDilIIK4pSCICDilIzilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilJAgIOKUjOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUkCAg4pSM4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSQICAg4pSCCuKUgiAg4pSCIOS4nOaWuei0ouWvjCAg4pSCICDilIIg5paw5rWq6LSi57uPICDilIIgIOKUgiDkuZDlkpXkuZDogqEgbGVndSAg4pSCICAg4pSCCuKUgiAg4pSCIHB1c2gyZGVsYXnilIIgIOKUgiBzaW5hIGhmXyAg4pSCICDilIIgd3d3LmxlZ3VsZWd1ICAg4pSCICAg4pSCCuKUgiAg4pSCICAgICAgICAgICDilIIgIOKUgiBzaW5hIGtsaW5l4pSCICDilIIgLmNvbSAgICAgICAgICAg4pSCICAg4pSCCuKUgiAg4pSU4pSA4pSA4pSA4pSA4pSs4pSA4pSA4pSA4pSA4pSA4pSYICDilJTilIDilIDilIDilIDilKzilIDilIDilIDilIDilIDilJggIOKUlOKUgOKUgOKUgOKUgOKUgOKUgOKUrOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUmCAgIOKUggrilIIgICAgICAg4pSCICAgICAgICAgICAgIOKUgiAgICAgICAgICAgICAgIOKUgiAgICAgICAgICAgIOKUggrilIIgICAgICAg4pa8ICAgICAgICAgICAgIOKWvCAgICAgICAgICAgICAgIOKWvCAgICAgICAgICAgIOKUggrilIIgIOKUjOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUkCAgICAg4pSCCuKUgiAg4pSCICAxMCDot6/lubblj5HmipPlj5Yg4oaSIGJ1aWxkX3NuYXBzaG90KCkgICAgICAgIOKUgiAgICAg4pSCCuKUgiAg4pSCICDCtyDmjIfmlbAv5rGH546HL+WVhuWTgSDCtyDlub/luqYv6LWE6YeR5rWBL+S8sOWAvCAgICDilIIgICAgIOKUggrilIIgIOKUgiAgwrcg6Ieq566XIFhYRkkgKyDlhrDngrkgICAgICAgICAgICAgICAgICAgIOKUgiAgICAg4pSCCuKUgiAg4pSU4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSs4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSYICAgICDilIIK4pSCICAgICAgICAgICAgICAgICAgIOKUgiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIOKUggrilIIgICAgICAgICAg4pSM4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pS04pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSQICAgICAgICAgICAgICAgICAgICAg4pSCCuKUgiAgICAgICAgICDilrwgICAgICAgICAgICAgICAgIOKWvCAgICAgICAgICAgICAgICAgICAgICDilIIK4pSCICDilIzilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilJAgIOKUjOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUkCAgICAgICAgICAgICAg4pSCCuKUgiAg4pSCIEtWICh3b3JrZXJzIOKUgiAg4pSCIEdpdEh1YiBBUEkgICAg4pSCICAgICAgICAgICAgICDilIIK4pSCICDilIIgIC5kZXYg6K+75Y+WKSAg4pSCICDilIIgUFVUIGRhdGEuanNvbiDilIIgICAgICAgICAgICAgIOKUggrilIIgIOKUlOKUgOKUgOKUgOKUgOKUgOKUgOKUrOKUgOKUgOKUgOKUgOKUgOKUgOKUmCAg4pSU4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSs4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSYICAgICAgICAgICAgICDilIIK4pSU4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pS84pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pS84pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSYCiAgICAgICAgICDilIIgICAgICAgICAgICAgICAgIOKUggogICAgICAgICAg4pa8ICAgICAgICAgICAgICAgICDilrwK4pSM4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSQICDilIzilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilJAK4pSCIHdvcmtlcnMuZGV2ICAgICDilIIgIOKUgiBHaXRIdWIgUGFnZXMgICAgICAgICAgICAgIOKUggrilIIgKOmcgFZQTikgICAgICAgICDilIIgIOKUgiBnaXRodWIuaW8gKOWbveWGheebtOi/nikgICAgICDilIIK4pSCIOivuyAvYXBpL2RhdGEgICAg4pSCICDilIIg6K+7IC4vZGF0YS5qc29uICjlj6ror7spICAgICDilIIK4pSCIOWQq+aJi+WKqOWIt+aWsOaMiemSriAgIOKUgiAg4pSCIOaXoOaJi+WKqOWIt+aWsCAgICAgICAgICAgICAgICDilIIK4pSU4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSYICDilJTilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilJgKYGBgCgoqKuaguOW/g+mAu+i+kSoq77yaV29ya2VyIOavjzMw5YiG6ZKf6YeH6ZuGMTDot6/mlbDmja7mupAg4oaSIOiuoeeulyBYWEZJICsg5Yaw54K5IOKGkiDlkIzml7blhpnlhaUgS1bvvIjkvpsgVlBOIOeJiOivu+WPlu+8ieWSjOaOqOmAgeWIsCBHaXRIdWIgUGFnZXPvvIjkvpvlm73lhoXnm7Tov57vvInjgIIKCi0tLQoKIyMg6Kem5Y+R6KeE5YiZCgp8IOinhOWImSB8IOWAvCB8CnwtLS18LS0tfAp8ICoqQ3Jvbioq77yIVVRD77yJIHwgYDQ1IDFgIC8gYDE1LDQ1IDItN2AgLyBgMTUgOGAgYCogKiBtb24tZnJpYO+8iDMg5q6177yM5YWxIDE0IOasoS/kuqTmmJPml6XvvIkgfAp8ICoqQ3Jvbioq77yI5YyX5Lqs77yJIHwgYDk6NDUsIDEwOjE1LCAxMDo0NSwg4oCmLCAxNjoxNWDvvIjmr48gMzAg5YiG6ZKf77yJIHwKfCAqKummluasoeinpuWPkSoqIHwg5YyX5LqsICoqOTo0NSoqIHwKfCAqKuacq+asoeinpuWPkSoqIHwg5YyX5LqsICoqMTY6MTUqKiB8CnwgKirkuqTmmJPml6UqKiB8ICoqQSDogqHkuqTmmJPml6UqKu+8iOmAmui/hyBsZWd1IOmhtemdoue7n+iuoeaXpeacn+iHquWKqOagoemqjO+8jOWQq+iwg+S8ke+8iSB8CnwgKirlkajmnKsqKiB8IOKdjCDot7Pov4cgfAp8ICoq6IqC5YGH5pelKiogfCDinYwg6Lez6L+H77yI5pil6IqCL+WbveW6hi/muIXmmI7nrYnvvIkgfAoK5Yi35paw5a6I5Y2r77yI5bey566A5YyW77yJ77yaCi0g5LuF5LulIGBpc190eF90b2RheSgpYCDliKTlrprmmK/lkKbkuLrkuqTmmJPml6XvvIhsZWd1IOmhtemdoue7n+iuoeaXpeacn+agoemqjO+8jOWQq+iwg+S8kS/oioLlgYfml6Xoh6rliqjot7Pov4fvvInvvJsKLSDnqpflj6PliKTmlq0gYGluX3RyYWRpbmdfd2luZG93KClgIOW3suenu+mZpO+8jOS4jeWGjeS9nOS4uueLrOeri+WuiOWNq+OAggoKLS0tCgojIyDmlbDmja7mnb/lnZcKCiMjIyDwn5OIIEEg6IKh77yINiDlj6rvvIkKCnwg5oyH5pWwIHwg5p2l5rqQIHwgc2VjaWQgfAp8LS0tfC0tLXwtLS18Cnwg5LiK6K+B5oyH5pWwIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMS4wMDAwMDFgIHwKfCDmt7Hor4HmiJDmjIcgfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAwLjM5OTAwMWAgfAp8IOWIm+S4muadv+aMhyB8IOS4nOaWuei0ouWvjCBwdXNoMmRlbGF5IHwgYDAuMzk5MDA2YCB8Cnwg5rKq5rexMzAwIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMS4wMDAzMDBgIHwKfCDnp5HliJs1MCB8IOS4nOaWuei0ouWvjCBwdXNoMmRlbGF5IHwgYDEuMDAwNjg4YCB8CnwgKirljJfor4E1MCoqIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMC44OTkwNTBgIHwKCiMjIyDwn5OKIOa4r+iCoe+8iDIg5Y+q77yJCgp8IOaMh+aVsCB8IOadpea6kCB8IHNlY2lkIHwKfC0tLXwtLS18LS0tfAp8IOaBkueUn+aMh+aVsCB8IOS4nOaWuei0ouWvjCBwdXNoMmRlbGF5IHwgYDEwMC5IU0lgIHwKfCDmgZLnlJ/np5HmioAgfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxMjQuSFNURUNIYCB8CgojIyMg8J+HuvCfh7gg576O6IKh77yIMyDlj6rvvIkKCnwg5oyH5pWwIHwg5p2l5rqQIHwgc2VjaWQgfAp8LS0tfC0tLXwtLS18Cnwg57qz5pav6L6+5YWLMTAwIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTAwLk5EWGAgfAp8IOagh+aZrjUwMCB8IOS4nOaWuei0ouWvjCBwdXNoMmRlbGF5IHwgYDEwMC5TUFhgIHwKfCDpgZPnkLzmlq8gfCDkuJzmlrnotKLlr4wgcHVzaDJkZWxheSB8IGAxMDAuREpJQWAgfAoKIyMjIPCfjI0g5YWo55CD77yINiDlj6rvvIkKCnwg5oyH5pWwIHwg5p2l5rqQIHwgc2VjaWQgfAp8LS0tfC0tLXwtLS18Cnwg5pel57uPMjI1IHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTAwLk4yMjVgIHwKfCDpn6nlm71LT1NQSSB8IOS4nOaWuei0ouWvjCBwdXNoMmRlbGF5IHwgYDEwMC5LUzExYCB8Cnwg5b635Zu9REFYIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTAwLkdEQVhJYCB8CnwgKirmrKfmtLLmlq/miZjlhYs2MDAqKiB8IOS4nOaWuei0ouWvjCBwdXNoMmRlbGF5IHwgYDEwMC5TWFhQYCB8CnwgKirms5Xlm71DQUM0MCoqIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTAwLkZDSElgIHwKfCAqKuiLseWbveWvjOaXtjEwMCoqIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTAwLkZUU0VgIHwKCiMjIyDwn5ui77iPIOWkp+Wul+WVhuWTge+8iDMg5Y+q77yJCgp8IOWTgeenjSB8IOadpea6kCB8IHN5bWJvbCB8CnwtLS18LS0tfC0tLXwKfCBXVEkg5Y6f5rK5IHwg5paw5rWqIGhmXF8gfCBgaGZfQ0xgIHwKfCBDT01FWCDpu4Tph5EgfCDmlrDmtaogaGZcXyB8IGBoZl9HQ2AgfAp8IOW4g+S8pueJueWOn+ayuSB8IOaWsOa1qiBoZlxfIHwgYGhmX09JTGAgfAoKIyMjIPCfkrEg5rGH546H77yIMSDlj6rvvIkKCnwg5ZOB56eNIHwg5p2l5rqQIHwgc2VjaWQgfAp8LS0tfC0tLXwtLS18Cnwg576O5YWD56a75bK45Lq65rCR5biBIHwg5Lic5pa56LSi5a+MIHB1c2gyZGVsYXkgfCBgMTMzLlVTRENOSGAgfAoKLS0tCgojIyDorqHnrpfmjIfmoIcKCiMjIyDlsI/ml63mgZDmg6fmjIfmlbDvvIhYWEZJ77yJCgrmgZDmg6cgNCDpobkgKyDotKrlqaogNSDpobnvvIzliqDmnYPlkIjmiJDmgZDmg6fmjIfmlbDvvIhYWEZJID0g5oGQ5oOn5oC75YiG77yJ44CC5LiOIFt4aWFveHUtZmVhcl0oaHR0cHM6Ly9naXRodWIuY29tL2hvbWphbm9uL3hpYW94dS1mZWFyKSDlrozlhajlkIzmupDorqHnrpfjgIIKCioq5oGQ5oOn5YiG6YeP77yI5YC86LaK6auY4oaS6LaK5oGQ5oOn4oaS5Y+N5ZCR55yL5aSa77yJ77yaKioKCnwg5YiG6YePIHwg5p2D6YeNIHwg5YWs5byPIHwg5pWw5o2u5p2l5rqQIHwKfC0tLXwtLS18LS0tfC0tLXwKfCDlm57mkqQgZHJhd2Rvd24gfCAwLjMwIHwgY2xhbXAoYWJzKGRkKcOXNTAwKSB8IOS4iuivgeaMh+aVsCAyMCDml6UgSyArIOW9k+aXpSBzcG9077yI5paw5rWq77yJIHwKfCDlub/luqYgYnJlYWR0aCB8IDAuMjUgfCBjbGFtcCgoZG93bi91cC0wLjUpw5cxMDApIHwg5LmQ5ZKV5LmQ6IKhIGxlZ3XvvIjlhajluILlnLrmtqjot4zlrrbmlbDvvIkgfAp8IOi3jOWBnOavlCBsaW1pdGRvd24gfCAwLjIwIHwgY2xhbXAoKGxkL2x1KcOXNTApIHwg5LmQ5ZKV5LmQ6IKhIGxlZ3XvvIjmtqjlgZwv6LeM5YGc5a625pWw77yJIHwKfCDms6Lliqjnjocgdm9sIHwgMC4yNSB8IGNsYW1wKHZvbF9wY3TDlzEwMCkgfCDkuIror4HmjIfmlbAgMjAg5pel5rOi5Yqo546H5YiG5L2N77yI5paw5rWq77yJIHwKCioq6LSq5amq5YiG6YeP77yI5YC86LaK6auY4oaS6LaK6LSq5amq4oaS5Y+N5ZCR55yL56m677yJ77yaKioKCnwg5YiG6YePIHwg5p2D6YeNIHwg5YWs5byPIHwg5pWw5o2u5p2l5rqQIHwKfC0tLXwtLS18LS0tfC0tLXwKfCDliqjph48gbW9tZW50dW0gfCAwLjI1IHwgY2xhbXAocmV0MjDDlzUwMCkgfCDkuIror4HmjIfmlbAgMjAg5pel5rao5bmF77yI5paw5rWq77yJIHwKfCDmtqjlgZzmr5QgbGltaXR1cCB8IDAuMTUgfCBjbGFtcCgobHUvbGQpw5c1MCkgfCDkuZDlkpXkuZDogqEgbGVndSB8Cnwg5pWj5oi36L+96auYIHJldGFpbGluIHwgMC4yMCB8IGNsYW1wKHJldGFpbF9uZXTDlzIwMCkgfCDkuJzmlrnotKLlr4wgdWxpc3TvvIjmsqrmt7HkuKTluILotYTph5HmtYHvvIkgfAp8IOi2heS5sCBvdmVyYm91Z2h0IHwgMC4yMCB8IGNsYW1wKGFib3Zlw5c1MDApIHwg5LiK6K+B5oyH5pWw6auY5LqOIDIwIOaXpeWdh+e6v+W5heW6piB8Cnwg6IOM56a7IGRpdmVyZ2VuY2UgfCAwLjIwIHwgY2xhbXAoNTDiiJJkaXbDlzIwMCkgfCDkuLvlipviiJLmlaPmiLfvvIjkuJzotKLotYTph5HmtYHvvIkgfAoKKipYWEZJIOS/oeWPt+mYiOWAvCoq77yaCgp8IFhYRkkgfCDkv6Hlj7cgfCDlkKvkuYkgfAp8LS0tfC0tLXwtLS18Cnwg4omlNzUgfCAqKkJVWSoqIHwg5p6B5bqm5oGQ5oOn77yM5YiG5om55L2O5ZC4IHwKfCA2MOKAkzc0IHwgKipBQ0NVTVVMQVRFKiogfCDmgZDmg6fvvIzpgKLkvY7lkLjnurMgfAp8IDQw4oCTNTkgfCAqKkhPTEQqKiB8IOS4reaAp++8jOaMieetlueVpeaMgeaciSB8CnwgMjXigJMzOSB8ICoqUkVEVUNFKiogfCDlgY/otKrlqarvvIzpgKLpq5jlh4/ku5MgfAp8IDwyNSB8ICoqU0VMTCoqIHwg5p6B5bqm6LSq5amq77yM5YeP5LuT6YG/6ZmpIHwKCiMjIyBBIOiCoeWGsOeCue+8iDQg57u05bqm77yM5YWo6YOo5ruh6LazID0g5Yaw54K577yJCgp8IOe7tOW6piB8IOmYiOWAvCB8IOaVsOaNrua6kCB8CnwtLS18LS0tfC0tLXwKfCBEMSDkuIvot4zlub/luqYgfCDkuIvot4wg4omlIDQwMDAg5LiUIOWNoOavlCDiiaUgODUlIHwgbGVndSDlhajluILlnLrmtqjot4zlrrbmlbAgfAp8IEQyIOaMh+aVsC9FVEYg6LeM5bmFIHwg5LiK6K+BIOKJpCAtMi4wJSDkuJQg5Yib5Lia5p2/IOKJpCAtMi41JSDkuJQgRVRGIOi3jCDiiaQgLTIuNSUg5Y2g5q+UIOKJpSA2MCUgfCDkuJzotKLmjIfmlbAgKyA0NSDlj6rmoLjlv4MgRVRGIOeZveWQjeWNlSB8CnwgRDMg6LeM5YGc5pWw6YePIHwg6LeM5YGcIOKJpSA1MCDkuJQg6LeM5YGcL+a2qOWBnCDiiaUgMyB8IGxlZ3Ug5rao5YGcL+i3jOWBnOWutuaVsCB8CnwgRDQg5pS+6YeP5oGQ5oWMIHwg5pS+6YeP5YCN5pWw77yI5b2T5pel5oiQ5Lqk6aKdL+i/kSAyMCDml6XlnYfpop3vvIniiaUgMS4zIHwg5Lic6LSiIHVsaXN077yI5b2T5pel6aKd77yJKyBLViDmu5rliqjnvJPlrZjvvIgyMOaXpeWdh+mine+8jOmmluasoeS7juiFvuiur25ld2Zxa2xpbmXooaXpvZDvvIkgfAoK5qC45b+DIEVURiDnmb3lkI3ljZXlhbEgKio0NSDlj6oqKu+8iOWuveWfuiAxMSDlj6ogKyDooYzkuJogMzQg5Y+q77yM5ZCr5Lit6K+BMjAwMCDlvq7nm5jjgIHkv53pmakv6IO95rqQL+S6pOi/kC/ljYrlr7zkvZPnrYnooaXlhYXpobnvvInjgIIKCiMjIyDwn4e68J+HuCDnvo7ogqHmgZDmhYzmjIfmlbAKCuWcqCBBIOiCoeWGsOeCueWNoeS4i+aWueWxleekuu+8jOWMheWQq+S4ieS4quaMh+agh++8mgoKfCDmjIfmoIcgfCDlkKvkuYkgfCDmlbDmja7mupAgfCDop6Por7vljLrpl7QgfAp8LS0tfC0tLXwtLS18LS0tfAp8ICoqVklYKiogfCDmoIfmma41MDAgMzDlpKnpmpDms6IgfCBZYWhvbyBGaW5hbmNlIHwg4omlNDDmnoHluqbmgZDmhYwgLyDiiaUzMOaBkOaFjCAvIOKJpTI15YGP5oGQ5oWMIC8g4omlMjDlgY/pq5ggLyDiiaUxNeato+W4uCAvIOKJpTEy5YGP5L2OIC8gPDEy5p6B5L2O6LSq5amqIHwKfCAqKlZYTioqIHwg57qz5pav6L6+5YWLMTAwIDMw5aSp6ZqQ5rOiIHwgWWFob28gRmluYW5jZSB8IOWQjOS4iu+8iOe6s+aMh+azouWKqOeOh+mAmuW4uOmrmOS6juagh+aZru+8iSB8CnwgKipDTk4gRmVhciAmIEdyZWVkKiogfCA35Zug5a2Q5oGQ5oOn6LSq5amq5oyH5pWw77yIMOKAkzEwMO+8iSB8IENOTiBkYXRhdml6IEFQSSB8IOKJpDI15p6B5bqm5oGQ5oWMIC8g4omkNDXmgZDmhYwgLyDiiaQ1NeS4reaApyAvIOKJpDc16LSq5amqIC8gPjc15p6B5bqm6LSq5amqIHwKCiMjIyDkvLDlgLzmsLTkvY0KCnwg5oyH5pWwIHwg5p2l5rqQIHwKfC0tLXwtLS18Cnwg5qCH5pmuNTAwIHwg6JuL5Y235Z+66YeRIGluZGV4X2V2YSB8Cnwg5Yib5Lia5p2/5oyHIHwg6JuL5Y235Z+66YeRIGluZGV4X2V2YSB8Cnwg5Lit6K+B57qi5Yip5L2O5rOiIHwg6JuL5Y235Z+66YeRIGluZGV4X2V2YSB8Cnwg5oGS55Sf56eR5oqAIHwg6JuL5Y235Z+66YeRIGluZGV4X2V2YSB8Cnwg5rKq5rexMzAwIHwg6JuL5Y235Z+66YeRIGluZGV4X2V2YSB8CgotLS0KCiMjIOaWh+S7tue7k+aehAoKYGBgCm1hcmtldC1saXZlLwrilJzilIDilIAgd29ya2VyLnB5ICAgICAgICAgICAgICAjIENsb3VkZmxhcmUgUHl0aG9uIFdvcmtlciDkuLvpgLvovpEK4pSCICAg4pSc4pSAIOmHh+mbhiA5IOi3r+aVsOaNrua6kCAgICAgIyAoSFRUUCDlubblj5EsIGFzeW5jaW8uZ2F0aGVyKQrilIIgICDilJzilIAgY29tcHV0ZV94eGZpKCkgICAgICAjIOWwj+aXreaBkOaDp+aMh+aVsOiuoeeulwrilIIgICDilJzilIAgY29tcHV0ZV9iaW5nZGlhbigpICAjIEEg6IKh5Yaw54K55Yik5a6aCuKUgiAgIOKUnOKUgCBwdWJsaXNoX3RvX2dpdGh1YigpICMg5o6oIGRhdGEuanNvbiDliLAgR2l0SHViCuKUgiAgIOKUlOKUgCBjbGFzcyBEZWZhdWx0ICAgICAgICMgV29ya2VyIOWFpeWPoyAoZmV0Y2ggKyBzY2hlZHVsZWQpCuKUnOKUgOKUgCB3cmFuZ2xlci50b21sICAgICAgICAgICMgQ2xvdWRmbGFyZSDpg6jnvbLphY3nva4K4pSCICAg4pSc4pSAIEtWIOe7keWumiAgICAgICAgICAgICAjIOW/q+eFp+WtmOWCqArilIIgICDilJzilIAgQVNTRVRTIOe7keWumiAgICAgICAgICMg6Z2Z5oCB6Z2i5p2/CuKUgiAgIOKUlOKUgCBDcm9uIOinpuWPkSAgICAgICAgICAgIyA0NSAxIC8gMTUsNDUgMi03IC8gMTUgOCAqICogbW9uLWZyaQrilJzilIDilIAgcHVibGljLwrilIIgICDilJTilIDilIAgaW5kZXguaHRtbCAgICAgICAgICMgV29ya2VyIOeJiOWJjeerr++8iFZQTiDlj6/nlKjvvIzlkKvmiYvliqjliLfmlrDmjInpkq7vvIkK4pSc4pSA4pSAIGRvY3MvICAgICAgICAgICAgICAgICAgIyBHaXRIdWIgUGFnZXMg5oqV6YCS55uu5b2VCuKUgiAgIOKUnOKUgOKUgCBpbmRleC5odG1sICAgICAgICAgIyBHaXRIdWIgUGFnZXMg54mI5YmN56uv77yI5Zu95YaF55u06L+e77yM5Y+q6K+75b+r54Wn77yJCuKUgiAgIOKUlOKUgOKUgCBkYXRhLmpzb24gICAgICAgICAgIyDlrp7ml7blv6vnhafvvIhXb3JrZXIg5q+PIDMwIOWIhumSn+iHquWKqOaOqOmAge+8iQrilJzilIDilIAgcHlwcm9qZWN0LnRvbWwgICAgICAgICAjIFB5dGhvbiDpobnnm67phY3nva4K4pSc4pSA4pSAIHB5bG9jay50b21sICAgICAgICAgICAgIyB1diDkvp3otZbplIEK4pSc4pSA4pSAIFJFQURNRS5tZCAgICAgICAgICAgICAgIyDmnKzmlofku7YK4pSU4pSA4pSAIERFUExPWV9TVEFUVVMubWQgICAgICAgIyDpg6jnvbLnirbmgIHkuI7mvJTov5vorrDlvZUKYGBgCgotLS0KCiMjIOaKgOacr+agiAoKfCDlsYIgfCDmioDmnK8gfAp8LS0tfC0tLXwKfCAqKuiuoeeul+W8leaTjioqIHwgQ2xvdWRmbGFyZSBQeXRob24gV29ya2Vy77yIUHlvZGlkZSDov5DooYzml7bvvIxgcHl0aG9uX3dvcmtlcnNgIOWFvOWuueagh+W/l++8iSB8CnwgKirmlbDmja7lrZjlgqgqKiB8IENsb3VkZmxhcmUgV29ya2VycyBLVu+8iOW/q+eFp+e8k+WtmO+8iSB8CnwgKirpnZnmgIHpnaLmnb8qKiB8IENsb3VkZmxhcmUgV29ya2VycyBBc3NldHPvvIjljp/nlJ8gSFRNTC9KU++8jOaXoOahhuaetu+8iSB8CnwgKirlm73lhoXmipXpgJIqKiB8IEdpdEh1YiBQYWdlc++8iOmbtuacjeWKoeWZqOi0ue+8jGdpdGh1Yi5pbyDkuK3lm73lhoXnm7Tov57vvIkgfAp8ICoq5pWw5o2u5o6o6YCBKiogfCBXb3JrZXIg4oaSIEdpdEh1YiBDb250ZW50cyBBUEkg4oaSIGBkb2NzL2RhdGEuanNvbmAgfAp8ICoq5Yet6K+B566h55CGKiogfCBHaXRIdWIgUEFUIOS7pSBDbG91ZGZsYXJlIFNlY3JldCDlvaLlvI/liqDlr4blrZjlgqjvvIzkuI3ov5vku6PnoIEgfAp8ICoqQ3JvbioqIHwgQ2xvdWRmbGFyZSBUcmlnZ2Vyc++8iOavjyAzMCDliIbpkp/vvIkgfAp8ICoq6YOo572y5bel5YW3KiogfCBgcHl3cmFuZ2xlcmDvvIh3b3JrZXJzLXB5IOKJpTEuOTDvvInihpIgYHdyYW5nbGVyIGRlcGxveWAgfAoKLS0tCgojIyDpg6jnvbLmjIfljZcKCmBgYGJhc2gKIyAxLiDlronoo4Xkvp3otZYKdXYgc3luYwoKIyAyLiDorr7nva4gR2l0SHViIFBBVO+8iOeUqOS6juaVsOaNruaOqOmAge+8iQplY2hvICLkvaDnmoRnaXRodWJfdG9rZW4iIHwgQ0xPVURGTEFSRV9BUElfVE9LRU49IuS9oOeahGNmX3Rva2VuIiBDTE9VREZMQVJFX0FDQ09VTlRfSUQ9IuS9oOeahGNmX2FjY291bnRfaWQiIHV2IHJ1biBweXdyYW5nbGVyIHNlY3JldCBwdXQgR0lUSFVCX1RPS0VOCgojIDMuIOmDqOe9siBXb3JrZXIKQ0xPVURGTEFSRV9BUElfVE9LRU49Ii4uLiIgQ0xPVURGTEFSRV9BQ0NPVU5UX0lEPSIuLi4iIHV2IHJ1biBweXdyYW5nbGVyIGRlcGxveQoKIyA0LiBHaXRIdWIgUGFnZXMg6K6+572u77yI5LuF6aaW5qyh77yJCiMg5LuT5bqTIFNldHRpbmdzIOKGkiBQYWdlcyDihpIgU291cmNlOiBEZXBsb3kgZnJvbSBicmFuY2gg4oaSIG1haW4g4oaSIC9kb2NzIOKGkiBTYXZlCmBgYAoKLS0tCgojIyDov5Dnu7Tor4rmlq3vvIhBUEnvvIkKCnwg56uv54K5IHwg5L2c55SoIHwKfC0tLXwtLS18CnwgYC9hcGkvZGF0YWAgfCDov5Tlm57lvZPliY3lv6vnhafvvIjnm7TmjqXor7vlj5YgS1bvvIzml6DpnIDph43mlrDmipPlj5bvvIkgfAp8IGAvYXBpL3JlZnJlc2hgIHwg5omL5Yqo5Yi35paw77ya5ZCM5q2l5p6E5bu65bm25YaZ5YWlIEtW44CB5o6o6YCBIEdpdEh1YiBQYWdlc++8iOetieS7t+S6jiBDcm9uIOi3keeahOmAu+i+ke+8iSB8CnwgYC9hcGkvY3Jvbl9kaWFnYCB8IOWumuaXtuinpuWPkeiviuaWre+8mui/lOWbnuacgOi/keS4gOasoSBDcm9uIOeahOeKtuaAge+8iGBlbnRlcmAgLyBgZGlzcGF0Y2hlZGAgLyBgZXJyb3Jg77yJ77yM5ZCrIGBpc190eGDjgIFgZXJyYOOAgWB0YmAg5a2X5q6177yIR2l0SHViIOaOqOmAgeeKtuaAgeingSBgX2doX2RpYWdg77yJ77yM55So5LqO5o6S5p+l4oCc6Ieq5Yqo6Kem5Y+R5rKh6LeR4oCd6Zeu6aKYIHwKCioqQ3JvbiDlgaXlo67mgKcqKu+8mmBzY2hlZHVsZWQoc2VsZiwgY29udHJvbGxlciwgZW52LCBjdHgpYCDmjIkgQ2xvdWRmbGFyZSDov5DooYzml7bnrb7lkI3mjqXmlLYgNCDkuKrkvY3nva7lj4LmlbDvvJvnu5HlrprkuIDlvovpgJrov4cgYHNlbGYuZW52YCDojrflj5bigJTigJTkvY3nva7lj4LmlbDph4znmoQgYGVudmAg5ZyoIFB5dGhvbiBXb3JrZXJzIOeahCBzY2hlZHVsZWQg5LiK5LiL5paH5bm26Z2e55yf5a6e57uR5a6a5a+56LGh77yM5pu+5a+86Ie0IEtWIC8gR2l0SHViIOWGmeaTjeS9nOmdmem7mOWksei0peOAguWFqOeoiyBgdHJ5L2V4Y2VwdGAg5oqK54q25oCB5YaZ5YWlIEtW77yaYF9jcm9uX2RpYWdg77yI5a6a5pe26Kem5Y+RIGBlbnRlcmAvYHNraXBwZWRgL2BkaXNwYXRjaGVkYC9gZXJyb3Jg77yJ5LiOIGBfZ2hfZGlhZ2DvvIhHaXRIdWIg5o6o6YCBIGBub190b2tlbmAvYGVycm9yYC9gb2tg77yJ77yMKirkuI3lho3pnZnpu5jlpLHotKUqKuOAggo+ICoqMjAyNi0wNy0yNCDkv67lpI3orrDlvZUqKu+8muKRoCBgc2NoZWR1bGVkYCDmlLnnlLEgYHNlbGYuZW52YCDlj5bnu5HlrprvvIjljp/kvY3nva7lj4LmlbAgYGVudmAg6Z2Z6buY5aSx5pWI77yJ77yb4pGhIOihpeWFqCBgR0lUSFVCX1RPS0VOYCBzZWNyZXTvvIxHaXRIdWIgUGFnZXMg6Ieq5Yqo5o6o6YCB5oGi5aSN77yb4pGiIOaWsOWiniBgX2Nyb25fZGlhZ2AgLyBgX2doX2RpYWdgIOiviuaWreeVmeeXleOAggoKCi0tLQoKIyMg5LiOIHhpYW94dS1mZWFyIOeahOWFs+ezuwoK5pys6aG555uu55qEIFhYRkkg6K6h566X5LiOIFt4aWFveHUtZmVhcl0oaHR0cHM6Ly9naXRodWIuY29tL2hvbWphbm9uL3hpYW94dS1mZWFyKSDlrozlhajlkIzmupDvvIjlhazlvI/jgIHmnYPph43jgIHpmIjlgLzjgIHmlofmnKzpgJDooYzkuIDoh7TvvInvvIzlt67lvILku4XlnKjkuo7vvJoKCi0geGlhb3h1LWZlYXIg5L2/55SoIGFrc2hhcmUg5Y+W5pWw77yI5pys5ZywL0dpdEh1YiBBY3Rpb25z77yJCi0gbWFya2V0LWxpdmUg5L2/55So55u06L+eIEhUVFAgQVBJIOWPluaVsO+8iENsb3VkZmxhcmUgV29ya2VyIOaXoCBha3NoYXJl77yJCi0geGlhb3h1LWZlYXIg6L6T5Ye65Li6IEdpdEh1YiBQYWdlcyDpnZnmgIHmiqXlkYoKLSBtYXJrZXQtbGl2ZSDkuLrlrp7ml7bmu5rliqjnnIvmnb/vvIjmr48gMzAg5YiG6ZKf5Yi35paw77yJCgotLS0KCiMjIOaVsOaNruadpea6kAoKfCDmnaXmupAgfCDnlKjpgJQgfCDljY/orq4gfAp8LS0tfC0tLXwtLS18CnwgKirkuJzmlrnotKLlr4wqKiBwdXNoMmRlbGF5IHwgQS/muK8v576OL+WFqOeQg+aMh+aVsOOAgeaxh+eOh+OAgei1hOmHkea1geOAgUVURuOAgUstbGluZe+8iOaIkOS6pOmine+8iSB8IOWFrOW8gCBIVFRQIEFQSSB8CnwgKirmlrDmtarotKLnu48qKiBzaW5hIGhmXF8gfCDlhajnkIPlpKflrpfllYblk4Hlrp7ml7booYzmg4UgfCDlhazlvIAgSFRUUCBBUEkgfAp8ICoq5paw5rWq6LSi57uPKiogSy1saW5lIEFQSSB8IOS4iuivgeaMh+aVsOaXpUvvvIjlm57mkqQv5rOi5Yqo546HL+WKqOmHj++8iSB8IOWFrOW8gCBIVFRQIEFQSSB8CnwgKirkuZDlkpXkuZDogqEqKiBsZWd1bGVndS5jb20gfCDlhajluILlnLrmtqjot4wv5rao5YGcL+i3jOWBnOWutuaVsO+8iOebmOmdouW5v+W6pu+8iSB8IOWFrOW8gOmhtemdouino+aekCB8CnwgKirom4vljbfln7rph5EqKiBkYW5qdWFuZnVuZHMuY29tIHwg5oyH5pWw5Lyw5YC8IFBFL1BCL+WIhuS9jS/ogqHmga/njocgfCDlhazlvIAgSFRUUCBBUEkgfAp8ICoqR2l0SHViKiogYXBpLmdpdGh1Yi5jb20gfCDmipXpgJIgZGF0YS5qc29uIOWIsCBQYWdlcyDku5PlupMgfCBPQXV0aCBQQVQgfAoKPiAqKuWFjei0o+WjsOaYjioq77ya5omA5pyJ5pWw5o2u5Z2H5p2l6Ieq5YWs5byA572R57uc5o6l5Y+j77yM5LuF5L6b56CU56m25Y+C6ICD77yM5LiN5p6E5oiQ5oqV6LWE5bu66K6u44CC5pWw5o2u5a6e5pe25oCn5Y+X6ZmQ5LqO5ZCE5rqQ5pu05paw6aKR546H44CCCg=="


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


async def fetch_treasury_futures():
    """内盘国债期货（新浪 nf_）。字段布局与外盘 hf_ 不同：名称在末尾，
    [3]=最新价；名称前的最后一个数值字段=昨结算价（涨跌幅基准）；[8]为卖价(常0)。"""
    syms = ",".join(TREASURY_FUTURES.values())
    url = f"https://hq.sinajs.cn/list={syms}"
    txt = await http_get(url, ref="https://finance.sina.com.cn")
    out = {}
    for line in txt.split(";"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, val = line.split("=", 1)
        sym = key.replace("var hq_str_", "").strip()
        name = {v: k for k, v in TREASURY_FUTURES.items()}.get(sym)
        if not name:
            continue
        parts = [pp.strip() for pp in val.strip('"').split(",")]
        # 内盘 nf_ 布局：名称在末尾。向后扫描名称前的最后一个数值字段作昨结算基准。
        try:
            last = float(parts[3])
            prev = None
            for i in range(len(parts) - 2, -1, -1):
                try:
                    prev = float(parts[i])
                    break
                except ValueError:
                    continue
            chg = round((last / prev - 1) * 100, 2) if prev else None
        except (ValueError, IndexError):
            last, chg = None, None
        out[name] = {"name": name, "price": last, "chg": chg,
                     "source": "sina nf_"}
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


async def fetch_us_quotes():
    """美股行情：指数/ETF/期货 全部取自 Yahoo Finance（v8 chart）。
    按指定顺序输出 8 项；单条失败降级为 None（无兜底源）。"""
    YAHOO = [
        ("标普500", "^GSPC"),
        ("纳斯达克100", "^NDX"),
        ("纳斯达克综合", "^IXIC"),
        ("道琼斯", "^DJI"),
        ("美国红利指数ETF(SCHD)", "SCHD"),
        ("半导体ETF", "SOXX"),
        ("标普500期货", "ES=F"),
        ("纳指100期货", "NQ=F"),
    ]
    ORDER = [n for n, _ in YAHOO]

    async def _yq(name, sym):
        # Yahoo v8 chart：指数(^开头)/ETF(SOXX,SCHD)/期货(ES=F,NQ=F) 统一取价
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym.replace('^','%5E').replace('=','%3D')}?interval=1d&range=1d"
        try:
            r = jload(await http_get(url, ref="https://finance.yahoo.com"))
            meta = (r.get("chart") or {}).get("result", [{}])[0].get("meta", {})
            price = meta.get("regularMarketPrice")
            prev = meta.get("previousClose") or meta.get("chartPreviousClose")
            chg = round((price / prev - 1) * 100, 2) if (price is not None and prev) else None
            return name, {"name": name, "price": price, "chg": chg, "source": "yahoo"}
        except Exception:
            return name, None

    results = await asyncio.gather(*[_yq(n, s) for n, s in YAHOO])
    ydict = dict(results)
    return [ydict.get(n) for n in ORDER]

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

    # 11 路抓取彼此独立 → 一次性并发，总耗时=最慢一路（广度），大幅压缩窗口
    (indices, commodities, hs300, vol_mult, breadth, fund,
     idx_chg, etf, valuation, us_fear, us_quotes, treasury) = await asyncio.gather(
        fetch_index_quotes(), fetch_commodities(), fetch_hs300_deriv(),
        fetch_sh_volume_mult(env), fetch_breadth(), fetch_fund_flow(),
        fetch_idx_chg(), fetch_etf_down_ratio(), fetch_valuation(),
        fetch_us_fear(), fetch_us_quotes(), fetch_treasury_futures())

    snap["indices"] = indices
    snap["indices"].update(treasury)   # 合并内盘国债期货（新浪 nf_）
    snap["commodities"] = commodities
    snap["valuation"] = valuation
    snap["us_quotes"] = us_quotes
    # 年内涨跌幅
    try:
        ytd_bases = await load_ytd_bases(env)
    except Exception:
        ytd_bases = {}
    attach_ytd(snap, ytd_bases)

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

