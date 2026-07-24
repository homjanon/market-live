# market-live · Cloudflare Worker 代码备份

> 本目录由自动化脚本从 Cloudflare Worker `market-live` 打包备份，非手工维护。
> 用途：代码回滚 / 审计。

- 备份时间（北京时间）：2026-07-24 22:49:52
- 来源版本（Cloudflare deployment id）：fetch-failed:JSONDecodeError
- 对应线上：https://market-live.homjanon.workers.dev
- 备份时 cron 状态：`*/30 * * * *`（验证期，全天候每 30 分）
- 备份内容：worker.py（主逻辑）、wrangler.toml（配置）、index.html（GitHub Pages 面板）

> 注意：本备份发生在「计划 B（cron 切回 mon-fri 9:45–16:15）」执行之前，
> 因此 wrangler.toml 仍为 `*/30` 验证期配置。
