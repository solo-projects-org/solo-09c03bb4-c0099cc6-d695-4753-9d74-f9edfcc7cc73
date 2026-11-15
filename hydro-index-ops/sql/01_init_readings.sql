-- ============================================================================
-- 水文数仓 · 水位明细样表初始化（容器首次启动时由 docker-entrypoint-initdb.d 执行）
-- 幂等：可重复执行（DROP CASCADE 仅用于初始化库，生产库请勿在已纳管库执行）
-- ============================================================================

-- 测站维表（样例）
CREATE TABLE IF NOT EXISTS stations (
    station_id   BIGINT PRIMARY KEY,
    station_code TEXT NOT NULL UNIQUE,
    station_name TEXT NOT NULL,
    basin        TEXT
);

-- 水位明细表
CREATE TABLE IF NOT EXISTS readings (
    id           BIGINT GENERATED ALWAYS AS IDENTITY,
    station_id   BIGINT NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,
    water_level  NUMERIC(8, 3) NOT NULL,          -- 水位，米
    discharge    NUMERIC(12, 3),                 -- 流量，立方米/秒（可空）
    quality      TEXT NOT NULL DEFAULT 'GOOD',   -- GOOD / SUSPECT / BAD
    revision     INTEGER,                        -- 修订号；无版本写入为 NULL
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_readings PRIMARY KEY (id),
    CONSTRAINT fk_readings_station FOREIGN KEY (station_id)
        REFERENCES stations (station_id)
);

COMMENT ON TABLE readings IS '水位明细（高频观测，持续写入；每次修订回传一行）';

-- 修订回传幂等约束：同站同时刻同修订号只入库一次。
-- 无版本旧行 revision 为 NULL，PostgreSQL 唯一索引中 NULL 互不相等，不受影响。
CREATE UNIQUE INDEX IF NOT EXISTS uq_readings_revision
    ON readings (station_id, observed_at, revision);

-- 当前观测：每站每观测时刻只保留已收到的最高修订。
-- reading_id 外键 + 与原始行同事务提交，保证当前观测绝不指向未提交/不存在的行。
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

COMMENT ON TABLE current_observations IS '当前观测（每站每时刻的最高修订，分析查询用）';

-- 10 个测站
INSERT INTO stations (station_id, station_code, station_name, basin)
SELECT s,
       'ST-' || lpad(s::text, 3, '0'),
       '水文站' || s,
       CASE WHEN s <= 5 THEN '长江流域' ELSE '黄河流域' END
FROM generate_series(1, 10) AS s
ON CONFLICT (station_id) DO NOTHING;

-- 样例明细：10 站 × 每站 10000 条 = 10 万行，约覆盖近 10000 小时
-- 用随机水位模拟观测序列，保证 CREATE INDEX 有可感知的构建时长
INSERT INTO readings (station_id, observed_at, water_level, discharge, quality)
SELECT s.station_id,
       now() - (g.t * INTERVAL '1 hour'),
       round((20 + 5 * sin(g.t / 24.0) + random() * 0.8)::numeric, 3),
       round((100 + random() * 900)::numeric, 3),
       CASE WHEN random() < 0.02 THEN 'SUSPECT' ELSE 'GOOD' END
FROM stations AS s
CROSS JOIN LATERAL generate_series(1, 10000) AS g(t)
WHERE NOT EXISTS (SELECT 1 FROM readings LIMIT 1);

-- 人工索引示例：验证恢复/清理流程绝不会误删非本系统创建的索引
CREATE INDEX IF NOT EXISTS idx_readings_ingested_manual
    ON readings (ingested_at);

ANALYZE readings;
