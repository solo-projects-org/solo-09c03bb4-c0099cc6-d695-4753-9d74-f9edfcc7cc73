# 修订写入与当前观测：语义与并发保证

测站会迟到回传：一个观测时刻的数据可能先后到达多个版本（修订）。分析查询
必须始终看到**已收到的最高修订**，绝不能退回旧水位；同时旧版本仍要作为
原始明细留档可查。

## 数据模型

一笔观测由 **(station_id, observed_at, revision)** 唯一识别。

```
readings                原始明细（每次修订回传一行，只增不改）
├── id                  主键
├── station_id          站号（外键 → stations）
├── observed_at         观测时刻
├── revision            修订号；无版本写入的旧行为 NULL
├── water_level / discharge / quality
└── ingested_at

    uq_readings_revision  UNIQUE (station_id, observed_at, revision)
    —— PostgreSQL 唯一索引中 NULL 互不相等，无版本旧行不受约束影响。

current_observations    当前观测（每站每时刻一行，只保留最高修订）
├── (station_id, observed_at)  主键
├── revision            当前生效的修订号
├── reading_id          外键 → readings.id（当前观测指向的原始行）
├── water_level / discharge / quality（与原始行一致的冗余副本，查询免回表）
└── updated_at
```

## 写入流程（POST /readings/revisions，单事务）

```
BEGIN
 ① INSERT INTO readings ... ON CONFLICT (station_id, observed_at, revision) DO NOTHING
   ├─ 插入成功  → 新修订：执行 ② 推进当前观测
   └─ 未插入    → 该修订已存在：SELECT 比对内容
        ├─ 内容一致 → 幂等返回 200（deduplicated=true），不写任何数据
        └─ 内容不同 → ROLLBACK，返回 409，数据不变
 ② INSERT INTO current_observations ... ON CONFLICT (station_id, observed_at)
    DO UPDATE SET ... WHERE current_observations.revision < EXCLUDED.revision
 ③ SELECT current_observations 读回当前观测（随响应返回）
COMMIT
```

响应：`201`（首次入库）/ `200`（同版同内容重试）/ `409`（同版异内容冲突）。
`current_updated` 表示本次写入是否推进了当前观测（旧修订迟到时为 false）。

内容比对在 SQL 内完成，数值列按列精度同舍入后做 `IS NOT DISTINCT FROM`
（`%(water_level)s::numeric(8,3)`），NULL 安全且不会因浮点表示差异误判。

## 关键保证如何落地

### 1. 同版重试不重复入库

`uq_readings_revision` 唯一索引是数据库层兜底。并发下同版写入只有一家真正
插入，其余在 `ON CONFLICT DO NOTHING` 上等待对方提交后转为幂等查询；
若对方回滚，则等待者自己成为首次写入。因此同版同内容重试永远 200，
且 `readings` 中同一 (站, 时刻, 修订号) 永远只有一行。

### 2. 同版异内容冲突且不改数据

冲突路径在比对不一致时直接 `ROLLBACK`——该事务没有任何写入生效，
原始行与当前观测都保持原样，返回 409 并附上已存内容与收到内容。

### 3. 当前观测只升不降（并发 / 旧修订迟到 / 写入失败）

- **旧修订迟到**：`DO UPDATE ... WHERE revision < EXCLUDED.revision`，
  更旧的修订不满足条件，当前观测不动（但原始行已入库可查）。
- **多实例并发写不同修订**：两个事务在 `current_observations` 的同一主键行上
  被行锁序列化；`ON CONFLICT DO UPDATE` 在 READ COMMITTED 下会等待对方提交后
  基于**最新已提交值**重新评估 WHERE 条件，因此无论到达顺序如何，
  最终留下的都是最高修订。应用层没有任何锁，两个 API 实例同时写也安全。
- **写入失败**：原始行插入与当前观测推进在同一事务，任一步失败整体回滚，
  当前观测保持写入前的值。

### 4. 当前观测不指向未提交的原始行

`current_observations.reading_id` 与对应 `readings` 行在**同一事务**插入并
一起提交；其他会话只能读到已提交的当前观测，其指向的原始行必然也已提交。
`reading_id` 上的外键进一步在数据库层禁止指向不存在的行。

### 5. 无版本读写完全保留

无版本写入（`POST /readings`）的 `revision` 为 `NULL`；唯一索引中 NULL 互不
冲突，同一站同一时刻可写多条无版本行，行为与引入修订前完全一致。
`GET /readings` 返回全部原始行（含每个修订与无版本行），新增可选
`revision` 过滤参数用于查旧版回传明细。

## 启动时的 schema 保证

`app/db.py` 在连接池初始化时先做**目录检查**（只读、不取表锁），只有发现
`revision` 列 / 唯一索引 / `current_observations` 表缺失时才执行幂等 DDL。
这样稳态下 API 重启不触碰 `readings` 的表锁——即使此时有长事务持有
`readings` 快照，启动与恢复也不会被阻塞（`ALTER TABLE` 即使列已存在也会
取 ACCESS EXCLUSIVE 锁，必须避开）。新库由 `sql/01_init_readings.sql`
直接建全 schema，两者内容一致。

## 查询

- `GET /readings/current?station_id=&observed_at=` — 当前观测；
  不给 `observed_at` 时返回该站全部时刻的当前观测（时刻倒序）。
- `GET /readings?station_id=&start=&end=&revision=` — 原始明细（含旧修订）。
