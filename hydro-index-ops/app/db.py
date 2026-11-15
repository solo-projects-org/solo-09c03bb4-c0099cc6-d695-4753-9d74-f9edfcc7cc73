"""psycopg3 异步连接池与批次表 DDL。"""
from __future__ import annotations

import logging

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import settings

log = logging.getLogger("hydro.db")

pool: AsyncConnectionPool | None = None

# 批次状态机：
#   PENDING    已受理，等待构建（含 CLEANING 清理失败后的待重试）
#   BUILDING   CREATE INDEX CONCURRENTLY 会话进行中
#   CANCELLING 已请求取消，等待构建会话结束并清理残件
#   CLEANING   构建失败/取消后正在 DROP 残件
#   SUCCEEDED  构建成功且索引有效（终态）
#   FAILED     构建失败且残件已清净（终态）
#   CANCELLED  取消完成且残件已清净（终态）
DDL_BATCH_TABLE = """
CREATE TABLE IF NOT EXISTS index_build_batches (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state            TEXT NOT NULL,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    build_pid        INTEGER,
    phase            TEXT,
    error            TEXT,
    cleanup_attempts INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_batch_state CHECK (
        state IN ('PENDING', 'BUILDING', 'CANCELLING', 'CLEANING',
                  'SUCCEEDED', 'FAILED', 'CANCELLED')
    )
);

-- 同一时刻只允许一个非终态批次（从数据库层面兜底“只受理一批”）
CREATE UNIQUE INDEX IF NOT EXISTS uq_batch_one_active
    ON index_build_batches ((1))
    WHERE state IN ('PENDING', 'BUILDING', 'CANCELLING', 'CLEANING');
"""

# 修订写入与当前观测：
#   - readings.revision 可空，无版本旧行为 NULL；PostgreSQL 唯一索引中 NULL
#     互不相等，因此无版本写入不受约束影响，行为完全保留。
#   - uq_readings_revision 兜底“同站同时刻同修订号只入库一次”，是幂等重试
#     与冲突检测的数据库层保证，多实例并发写入同样安全。
#   - current_observations 每站每时刻只保留已收到的最高修订；reading_id
#     外键 + 同事务提交保证当前观测永远指向一条已提交的原始行。
DDL_REVISION_TABLES = """
ALTER TABLE readings ADD COLUMN IF NOT EXISTS revision INTEGER;

CREATE UNIQUE INDEX IF NOT EXISTS uq_readings_revision
    ON readings (station_id, observed_at, revision);

CREATE TABLE IF NOT EXISTS current_observations (
    station_id   BIGINT NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,
    revision     INTEGER NOT NULL,
    reading_id   BIGINT NOT NULL,
    water_level  NUMERIC(8, 3) NOT NULL,
    discharge    NUMERIC(12, 3),
    quality      TEXT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_current_observations PRIMARY KEY (station_id, observed_at),
    CONSTRAINT fk_current_reading FOREIGN KEY (reading_id)
        REFERENCES readings (id)
);
"""

# 修订 schema 是否已就绪的目录检查（只读，不取表锁）。
# 注意：ALTER TABLE 即使列已存在也会取 ACCESS EXCLUSIVE 锁，会被持有
# readings 快照的长事务挡住；因此只有目录检查发现缺件时才执行 DDL，
# 稳态下启动路径零锁，API 重启恢复不会被长事务阻塞。
DDL_REVISION_CHECK = """
SELECT
    EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'readings' AND column_name = 'revision'
    ) AS has_revision_col,
    to_regclass('uq_readings_revision') IS NOT NULL AS has_revision_uq,
    to_regclass('current_observations') IS NOT NULL AS has_current_table
"""


async def init_pool() -> AsyncConnectionPool:
    global pool
    pool = AsyncConnectionPool(
        conninfo=settings.pg_dsn,
        min_size=2,
        max_size=10,
        kwargs={"row_factory": dict_row, "autocommit": False},
        check=AsyncConnectionPool.check_connection,
        open=False,
    )
    await pool.open()
    async with pool.connection() as conn:
        await conn.execute(DDL_BATCH_TABLE)
        cur = await conn.execute(DDL_REVISION_CHECK)
        ready = await cur.fetchone()
        if not (
            ready["has_revision_col"]
            and ready["has_revision_uq"]
            and ready["has_current_table"]
        ):
            log.info("revision schema incomplete, applying DDL: %s", ready)
            await conn.execute(DDL_REVISION_TABLES)
        await conn.commit()
    log.info("connection pool ready, batch/revision tables ensured")
    return pool


async def close_pool() -> None:
    if pool is not None:
        await pool.close()
        log.info("connection pool closed")
