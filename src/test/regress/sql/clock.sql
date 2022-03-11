

--
-- Check the clock is *increasing*
--
SELECT * FROM get_cluster_clock() \gset t1
SELECT * FROM get_cluster_clock() \gset t2
SELECT * FROM get_cluster_clock() \gset t3
SELECT :t2get_cluster_clock > :t1get_cluster_clock AS t2_greater_t1;
SELECT :t3get_cluster_clock > :t2get_cluster_clock AS t3_greater_t2;

--
-- Check if the latest clock value is persisted in the catalog
--
SELECT logical_clock_value FROM pg_dist_local_group \gset t1
/* get the current clock value */
SELECT * FROM get_cluster_clock() \gset t2
/* wait for the daemon to save the clock */
SELECT pg_sleep(3);
/* fetch the value from the catalog */
SELECT logical_clock_value FROM pg_dist_local_group \gset t3

SELECT :t3logical_clock_value > :t1logical_clock_value AS t3_greater_t1;
/* the value returned by get_cluster_clock() must be persisted in the catalog */
SELECT :t2get_cluster_clock = :t3logical_clock_value AS t2_saved_in_t3;


--
-- Check the value returned by get_cluster_clock is close to Epoch in ms
--
SELECT (extract(epoch from now()) * 1000)::bigint AS epoch \gset
SELECT get_cluster_clock() \gset
SELECT (:get_cluster_clock - :epoch) < 100;

CREATE TABLE dist_table (id int, nonid int);
SELECT create_distributed_table('dist_table', 'id', colocate_with := 'none');

SHOW citus.enable_global_clock; -- Default should be false
BEGIN;
INSERT INTO dist_table SELECT generate_series(1, 10000, 1), 0;
SET client_min_messages TO DEBUG1;
COMMIT;

-- Turn on the global clock, commit should pick the greatest clock value
SET citus.enable_global_clock to ON;
BEGIN;
INSERT INTO dist_table SELECT generate_series(1, 10000, 1), 0;
SET client_min_messages TO DEBUG1;
COMMIT;

RESET client_min_messages;
RESET citus.enable_global_clock;
