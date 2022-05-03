
DROP FUNCTION columnar.get_storage_id(regclass);
DROP VIEW columnar.options;
DROP VIEW columnar.stripe;
DROP VIEW columnar.chunk_group;
DROP VIEW columnar.chunk;

DROP SCHEMA columnar;

ALTER SCHEMA columnar_internal RENAME TO columnar;
GRANT USAGE ON SCHEMA columnar TO PUBLIC;
GRANT SELECT ON columnar.options TO PUBLIC;
GRANT SELECT ON columnar.stripe TO PUBLIC;
GRANT SELECT ON columnar.chunk_group TO PUBLIC;
