# 性能与 API 优化 backlog

记录本次 fork 已经做掉的性能/兼容性改动，以及**有意留到以后**的事项。
写下来是为了避免下次从零推理，也为了避免把 SQLite 这类大件在没设计好的情况下塞进来。

状态：✅ 已完成 · ⬜ 待办（未开始）· ⚠️ 有前置依赖

---

## ✅ 已完成（本次 fork）

| 项 | 提交 | 说明 |
|---|---|---|
| 并发从 1 放开 | `4fdeb49` | `threads` 默认 1 → 8，语义改为"每主机"，下限 4；图片独立预算 6 |
| 图片 HiDPI 档位 | `d987680` | `ImageTarget`（card/backdrop/avatar）×`scale_factor`，尺寸进缓存键 |
| 图片单飞 | `d987680` | `moka::future::Cache::try_get_with`，同一张图只下一次 |
| 多线路 + 测速 + 故障转移 | `fd568f9` | `ServerDomains` 解析、粘性切换、15 s 冷却、UI 测速/连通性 |
| 上报去重 + 关窗 drain | `00dd89c` | 水位 2 s / keepalive 60 s、串行化、失败退避、窗口关闭补一次 Stop |
| 服务器探测与降级 | `de39386` | base URL 探测（含重定向）、flavor、能力位、认证协商、`Fields` 常量、参数降级重试 |

基线口径（做后续任何一项前先补测，见文末）：

```bash
RUST_LOG=tsukimi=debug ./tsukimi 2>&1 | ts '%Y-%m-%d %H:%M:%.S' > /tmp/base.log
```

---

## ⬜ 1. 本地 SQLite 投影（收益最大，成本也最大）

> 现状：只有 `fetch_with_cache` 的 JSON 文件缓存（`CachePolicy::ReadCacheAndRefresh`）。真要"秒开"，必须本地库先出数据、再增量刷新。

**为什么先记着不做**：它不是一个"加个 sqlite 依赖"的任务，而是一次数据模型重构。没有下面这些配套，做出来就是屎山：

| 必须先想清楚 | 具体要求 |
|---|---|
| 归一化模型 | 先有 `media / files / sources / states` 这类**与服务器无关**的模型；否则只是把服务器 JSON 换个地方存 |
| 迁移 | `PRAGMA user_version` + 每版迁移脚本，必须可回滚；schema 变更和新字段一起上线 |
| 写入并发 | 单写者（专用写线程或 `Mutex<Connection>`）、`journal_mode=WAL`、`busy_timeout`，否则随机 `database is locked` |
| 索引 | 按真实查询建（首页 `homeCategory+updatedAt`、`file_source`、`states(key)`…），不建索引的首屏照样慢 |
| 失效 | 服务器 `tag`/revision 变化、条目删除、离线；用 `missing` 标记而不是删行 |
| 容量 | 图片缓存与 DB 缓存分开管理；`PRAGMA optimize`、`wal_checkpoint`、`VACUUM` 的触发条件 |
| 一致性 | 这是主要 bug 来源：先写测试（离线打开、切服务器、服务端删条目），再写实现 |

**如果将来要做，建议的切入顺序**（每步都能独立回退）：

1. 只缓存**首页/列表**的 `List` 结果：key = 请求路径+参数 hash + schema 版本，TTL 分级。
   ~100 行，拿到大部分体感收益，出错也只影响"是否显示旧数据"。
2. 再缓存**详情页**的 `SimpleListItem`（按 item id + revision）。
3. 最后才考虑跨服务器的统一投影（此时才需要 `sources/files/bindings/states` 那套表）。

**参照**：SenPlayer 的 `DataKit/LibraryKit` 那套表（`sources → files → media → bindings → states` + `change_log/sync_outbox/migration_map`），思路可看，**不要照抄表结构**——它的复杂度来自多端同步，我们单机不需要。

⚠️ 前置：无（可以独立做），但强烈建议在 2/3/4 完成后再评估，那时收益会更清楚。

---

## ⬜ 2. 滚动预取

- 位置：`ui/widgets/single_grid`、`hortu_scrolled` 等滚动容器。
- 做法：可视区之后 1–2 屏的卡片提前 `resolve_picture_file_scaled`。
- 注意：
  - 预取要**低优先级**，不能抢占可视区那 6 个图片预算（建议独立 2 个许可，或可视区优先队列）。
  - 必须可取消（快速滚动时）。
- 体量：~60 行。风险低。

---

## ⬜ 3. 批量请求（`Items?Ids=` + `Fields` 三档）

- 现状：详情页的相似/合集、季列表都是逐条请求；`Fields` 已经收敛成常量（`compat::fields`），但还没做批量。
- 做法：
  - 新增 `get_items_by_ids(ids, fields)` → `Users/{uid}/Items?Ids=a,b,c&Fields=…`
  - 季列表 / 缺失剧集 / 相似推荐各用一次批量替代 N 次。
- 注意：**部分自研服务器对 `Ids` 支持很差**，必须走 `compat` 的降级路径（去掉参数重试一次），否则反而更慢。
- 体量：~80 行。风险中（依赖 §compat 降级）。

---

## ⬜ 4. 负缓存（缺图/404）

- 现状：`get_image` 每次 404 都会重新请求；缺海报的条目在滚动时会反复打服务器。
- 做法：`moka` 记 `(cache_key) -> NotFound`，TTL 5–10 分钟。
- 注意：**只缓存 404/空 body**，不能缓存 401、超时、网络错误（否则服务端修好了也不恢复）。
- 体量：~20 行。风险低。

---

## ⬜ 5. `next_up` 的 N+1

- 现状：`get_next_up_merged` 每部剧一次 `Shows/{id}/Episodes?AdjacentTo=`（有 moka 缓存，TTL 2 h）
- 可选做法：
  - 服务端已经在 `Shows/NextUp` 里给 `UserData.LastPlayedDate` 时直接用，跳过查询；
  - 或用 `SortBy=DatePlayed&SortOrder=Descending&Limit=1` 一次拿到；
  - 都需要在真实 Jellyfin / Emby / 自研服务器上验证字段是否可靠。
- 注意：并发放开（`4fdeb49`）之后这项收益已经变小，优先级最低。

---

## 验收基线（每做一项前后各测一次）

在真机上记录这几个数字，写进 issue/PR 描述或本文末尾：

| 指标 | 怎么测 |
|---|---|
| 冷启动 → 首屏海报全部可见 | 录屏或日志里最后一张 `GET Items/*/Images/*` 的时间戳 |
| 首屏请求数 / 总耗时 | `RUST_LOG=tsukimi=debug` 统计同时间窗的请求条数 |
| 详情页 overview 可见 | 日志里 `Items/{id}` 返回的时间 |
| 重复请求数 | 同一 `cache_key` 出现次数（单飞/负缓存前后对比） |

---

## 踩过的坑（务必记住）

**`SETTINGS` 只能在主线程访问。** `ui/models/settings.rs` 里包的是
`glib::ThreadGuard<gio::Settings>`，`Deref` 里是**无条件 assert**：

```
Value accessed from different thread than where it was created
```

任何在 `spawn_tokio` / tokio worker 里读 `SETTINGS.*` 的代码都会 panic，而
`spawn_tokio` 的 `.unwrap()`（`utils.rs:25`）会把 panic 变成 `JoinError` 刷屏，
表现是"请求全部失败、海报永远转圈"。`app.rs` 里为此在启动时
`Lazy::force(&JELLYFIN_CLIENT)` / `Lazy::force(&DEVICE_ID)`。

推论：

- 需要在请求路径里用到设置值时，在 `JellyfinClient::default()`（主线程）里
  **读一次存成普通字段**，别在 `send_request` 里现读。
- 需要在 tokio 里写设置（例如持久化 account profile）时，用
  `crate::utils::spawn(async move { SETTINGS... })` 丢回主线程。

## 备注

- 上面所有"参照 SenPlayer"的部分都来自对它的二进制/字符串分析，属**推断**，落地前按自己的服务器实测。
- 真要说经验：**默认值比优化重要**。这次体感慢的元凶就是 `threads` 默认 1，而不是任何算法。
