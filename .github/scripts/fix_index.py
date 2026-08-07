#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Actions 自动修复脚本：把 Worker 推送的旧版 index.html 升级为最终版。

触发场景：Worker 每次 Cron 会把 public/index.html（旧版 IS_GH hostname 判断）覆盖
推送到 docs/index.html。本脚本在 GitHub 端检测到旧版特征后，就地替换为最终版：

1) load() 改双路径容错（先 ./data.json 再 /api/data），不依赖 hostname，
   github.io / 自定义域名 / workers.dev 三种环境全部自适应。
2) IS_GH 固定为 true（docs/ = GitHub Pages 只读快照版，隐藏刷新按钮）。

幂等性：若 index.html 已是最终版（不含旧版特征串），脚本直接退出、不产生 diff。
"""
import sys
from pathlib import Path

TARGET = Path("docs/index.html")

# 旧版特征串（Worker 推送的 public/index.html 中 load() 内的一行）
OLD_MARK = "const _url = (IS_GH ? './data.json' : '/api/data') + '?_=' + Date.now();"

# 旧版 IS_GH 判断行（Worker 推送版）
OLD_ISGH = "const IS_GH = location.hostname.includes('github.io');"

# 最终版 IS_GH 行
NEW_ISGH = "const IS_GH = true; // docs/ = GitHub Pages 只读快照版，始终隐藏刷新按钮"

# 最终版 load() 数据加载块
NEW_BLOCK = """    // 双路径容错：先读本地快照 ./data.json，失败再回退 /api/data（Worker）。
    // 不依赖 hostname 判断，自定义域名 / github.io / workers.dev 全部自适应。
    const _urls = ['./data.json', '/api/data'];
    let data = null, lastErr = null;
    for(const _u of _urls){
      try{
        const r = await fetch(_u + '?_=' + Date.now(), {cache:'no-store'});
        const txt = await r.text();
        data = JSON.parse(txt);
        if(data.error){ throw new Error(data.error); }
        break;
      }catch(e){ lastErr = e; }
    }
    if(!data) throw lastErr;"""


def main() -> int:
    if not TARGET.exists():
        print("NO_CHANGE: docs/index.html not found")
        return 0

    s = TARGET.read_text(encoding="utf-8")
    if OLD_MARK not in s:
        print("NO_CHANGE: already fixed")
        return 0

    old_block = f"""    const _url = (IS_GH ? './data.json' : '/api/data') + '?_=' + Date.now();
    const r = await fetch(_url, {{cache:'no-store'}});
    const txt = await r.text();
    const data = JSON.parse(txt);
    if(data.error){{ throw new Error(data.error); }}"""

    if old_block not in s:
        print("NO_CHANGE: old marker found but block mismatch, skip to avoid corruption")
        return 0

    s = s.replace(old_block, NEW_BLOCK, 1)
    # 同步把旧版 IS_GH 判断替换为固定 true
    s = s.replace(OLD_ISGH, NEW_ISGH, 1)
    TARGET.write_text(s, encoding="utf-8")
    print("FIXED: index.html upgraded to final dual-path version")
    return 0


if __name__ == "__main__":
    sys.exit(main())
