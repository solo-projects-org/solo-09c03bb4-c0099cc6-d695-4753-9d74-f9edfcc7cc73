-- 人工观察与排障速查（容器内执行：docker exec -it hydro-db psql -U hydro -d hydro_warehouse）

-- 1. 批次状态
SELECT id, state, cancel_requested, build_pid, phase, error, created_at, updated_at
FROM index_build_batches ORDER BY id;

-- 2. CIC 构建进度（phase 即构建阶段）
SELECT st.pid,
       a.state AS backend_state,
       a.wait_event_type,
       a.wait_event,
       st.phase,
       st.blocks_total,
       st.blocks_done,
       round(100.0 * st.blocks_done / NULLIF(st.blocks_total, 0), 2) AS pct,
       st.tuples_total,
       st.tuples_done
FROM pg_stat_progress_create_index st
JOIN pg_stat_activity a ON a.pid = st.pid;

-- 3. 阻塞关系（谁挡住了索引构建）
SELECT blocked.pid  AS blocked_pid,
       blocked.query AS blocked_query,
       blocking.pid  AS blocking_pid,
       blocking.query AS blocking_query,
       blocking.xact_start AS blocking_xact_start
FROM pg_stat_activity blocked
JOIN pg_stat_activity blocking
  ON blocking.pid = ANY (pg_blocking_pids(blocked.pid))
WHERE blocked.query ILIKE '%CREATE INDEX CONCURRENTLY%';

-- 4. readings 上的索引（valid=true 才可用，INVALID 是失败残件，必须 DROP）
SELECT indexname, indexdef
FROM pg_indexes WHERE tablename = 'readings';

SELECT i.relname AS index_name, ix.indisvalid, ix.indisready
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
WHERE t.relname = 'readings';

-- 5. 普通读写验证（构建期间应正常）
INSERT INTO readings (station_id, observed_at, water_level)
VALUES (1, now(), 21.500) RETURNING id;

SELECT * FROM readings WHERE station_id = 1 ORDER BY observed_at DESC LIMIT 5;

-- 6. 修订回传与当前观测
-- 某站某时刻的全部修订原始明细（旧版回传留档）
SELECT id, revision, water_level, discharge, quality, ingested_at
FROM readings
WHERE station_id = 1 AND observed_at = '2030-01-01T00:00:00Z'
ORDER BY revision;

-- 当前观测（每站每时刻的最高修订）与其指向的原始行是否一致
SELECT c.station_id, c.observed_at, c.revision, c.water_level AS current_level,
       r.water_level AS reading_level, r.id AS reading_id
FROM current_observations c
JOIN readings r ON r.id = c.reading_id
ORDER BY c.updated_at DESC
LIMIT 20;

-- 一致性体检：当前观测指向不存在/不匹配原始行的数量（应恒为 0）
SELECT count(*) AS inconsistent
FROM current_observations c
LEFT JOIN readings r ON r.id = c.reading_id
WHERE r.id IS NULL OR r.revision IS DISTINCT FROM c.revision
   OR r.station_id <> c.station_id OR r.observed_at <> c.observed_at;
