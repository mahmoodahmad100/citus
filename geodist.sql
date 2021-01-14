-- initialize a geo distributed partition without creation support in citus

CREATE FUNCTION create_range_shard(table_name regclass, start_value text, end_value text)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    new_shard_id bigint := master_create_empty_shard(table_name::text);
BEGIN
    UPDATE pg_dist_shard
    SET shardminvalue = start_value::text, shardmaxvalue = end_value::text
    WHERE shardid = new_shard_id;

    RETURN new_shard_id;
END;
$$;

CREATE TABLE belgium_planet_osm_roads_org(
  k integer PRIMARY KEY,
  osm_id bigint,
  way geometry(LineString,3857)
);
\copy belgium_planet_osm_roads_org FROM 'belgium_planet_osm_roads_org.csv' DELIMITER ',';

CREATE TABLE belgium_planet_osm_roads_dist(
  k integer PRIMARY KEY,
  osm_id bigint,
  way geometry(LineString,3857)
);
SELECT create_distributed_table('belgium_planet_osm_roads_dist', 'k');
INSERT INTO belgium_planet_osm_roads_dist SELECT * FROM belgium_planet_osm_roads_org;
CREATE INDEX ON belgium_planet_osm_roads_dist USING gist (way);

CREATE TABLE belgium_planet_osm_roads_geo(
  shard_id integer,
  k integer,
  osm_id bigint,
  way geometry,

  PRIMARY KEY (shard_id, k)
);
SELECT create_distributed_table('belgium_planet_osm_roads_geo', 'shard_id', distribution_type => 'range');
SELECT create_range_shard('belgium_planet_osm_roads_geo'::regclass, i::text, i::text) FROM generate_series(1,30) AS i;
CREATE INDEX ON belgium_planet_osm_roads_geo USING gist (way);

CREATE TABLE myshards (id SERIAL, bbox geometry);
INSERT INTO myshards (bbox) VALUES
('0103000020110F00000100000005000000A4703D0A65740C413E0AD7A758565941A4703D0A65740C411F85EB814DAD59413E0AD723F29515411F85EB814DAD59413E0AD723F29515413E0AD7A758565941A4703D0A65740C413E0AD7A758565941'),
('0103000020110F000001000000050000000AD7A3B0B82622413E0AD7A7585659410AD7A3B0B82622411F85EB814DAD59410000008098D425411F85EB814DAD59410000008098D425413E0AD7A7585659410AD7A3B0B82622413E0AD7A758565941'),
('0103000020110F000001000000050000000AD7A3B0B8262241EC51B860E9D358410AD7A3B0B8262241A4703D97A6E9584185EB5198A8FD2341A4703D97A6E9584185EB5198A8FD2341EC51B860E9D358410AD7A3B0B8262241EC51B860E9D35841'),
('0103000020110F00000100000005000000295C8FC2B1F11C4152B89E79F4765941295C8FC2B1F11C41AE47E114D3815941A4703DAAA1C81E41AE47E114D3815941A4703DAAA1C81E4152B89E79F4765941295C8FC2B1F11C4152B89E79F4765941'),
('0103000020110F00000100000005000000343333F3D1431941EC51B860E9D35841343333F3D1431941A4703D97A6E95841295C8FC2B1F11C41A4703D97A6E95841295C8FC2B1F11C41EC51B860E9D35841343333F3D1431941EC51B860E9D35841'),
('0103000020110F00000100000005000000666666B629DD1D41703D8A9FFF1F5941666666B629DD1D412A5C0FD6BC355941A4703DAAA1C81E412A5C0FD6BC355941A4703DAAA1C81E41703D8A9FFF1F5941666666B629DD1D41703D8A9FFF1F5941'),
('0103000020110F000001000000050000003E0AD723F29515413E0AD7A7585659413E0AD723F2951541AE47E114D3815941B91E850BE26C1741AE47E114D3815941B91E850BE26C17413E0AD7A7585659413E0AD723F29515413E0AD7A758565941'),
('0103000020110F00000100000005000000295C8FC2B1F11C41EC51B860E9D35841295C8FC2B1F11C41A4703D97A6E958418FC2F5C8C84F2041A4703D97A6E958418FC2F5C8C84F2041EC51B860E9D35841295C8FC2B1F11C41EC51B860E9D35841'),
('0103000020110F00000100000005000000B91E850BE26C174186EB51719B405941B91E850BE26C17413E0AD7A758565941AE47E1DAC11A1B413E0AD7A758565941AE47E1DAC11A1B4186EB51719B405941B91E850BE26C174186EB51719B405941'),
('0103000020110F00000100000005000000343333F3D1431941A4703D97A6E95841343333F3D14319415C8FC2CD63FF58418FC2F5C8C84F20415C8FC2CD63FF58418FC2F5C8C84F2041A4703D97A6E95841343333F3D1431941A4703D97A6E95841'),
('0103000020110F00000100000005000000B91E850BE26C17413E0AD7A758565941B91E850BE26C1741AE47E114D3815941295C8FC2B1F11C41AE47E114D3815941295C8FC2B1F11C413E0AD7A758565941B91E850BE26C17413E0AD7A758565941'),
('0103000020110F000001000000050000003E0AD723F29515419A9999197A5158413E0AD723F29515417B14AEF36EA858410000008098D425417B14AEF36EA858410000008098D425419A9999197A5158413E0AD723F29515419A9999197A515841'),
('0103000020110F000001000000050000003E0AD723F2951541AE47E114D38159413E0AD723F29515411F85EB814DAD59410AD7A3B0B82622411F85EB814DAD59410AD7A3B0B8262241AE47E114D38159413E0AD723F2951541AE47E114D3815941'),
('0103000020110F00000100000005000000AE47E1DAC11A1B415C8FC2CD63FF5841AE47E1DAC11A1B413E0AD7A758565941295C8FC2B1F11C413E0AD7A758565941295C8FC2B1F11C415C8FC2CD63FF5841AE47E1DAC11A1B415C8FC2CD63FF5841'),
('0103000020110F00000100000005000000A4703D0A65740C415C8FC2CD63FF5841A4703D0A65740C413E0AD7A7585659413E0AD723F29515413E0AD7A7585659413E0AD723F29515415C8FC2CD63FF5841A4703D0A65740C415C8FC2CD63FF5841'),
('0103000020110F000001000000050000003E0AD723F29515415C8FC2CD63FF58413E0AD723F29515413E0AD7A758565941B91E850BE26C17413E0AD7A758565941B91E850BE26C17415C8FC2CD63FF58413E0AD723F29515415C8FC2CD63FF5841'),
('0103000020110F000001000000050000000AD7A3B0B8262241A4703D97A6E958410AD7A3B0B826224114AE47042115594185EB5198A8FD234114AE47042115594185EB5198A8FD2341A4703D97A6E958410AD7A3B0B8262241A4703D97A6E95841'),
('0103000020110F00000100000005000000295C8FC2B1F11C419A99194337615941295C8FC2B1F11C4152B89E79F4765941A4703DAAA1C81E4152B89E79F4765941A4703DAAA1C81E419A99194337615941295C8FC2B1F11C419A99194337615941'),
('0103000020110F00000100000005000000295C8FC2B1F11C412A5C0FD6BC355941295C8FC2B1F11C419A99194337615941A4703DAAA1C81E419A99194337615941A4703DAAA1C81E412A5C0FD6BC355941295C8FC2B1F11C412A5C0FD6BC355941'),
('0103000020110F00000100000005000000B91E850BE26C17415C8FC2CD63FF5841B91E850BE26C174186EB51719B405941AE47E1DAC11A1B4186EB51719B405941AE47E1DAC11A1B415C8FC2CD63FF5841B91E850BE26C17415C8FC2CD63FF5841'),
('0103000020110F00000100000005000000295C8FC2B1F11C41703D8A9FFF1F5941295C8FC2B1F11C412A5C0FD6BC355941666666B629DD1D412A5C0FD6BC355941666666B629DD1D41703D8A9FFF1F5941295C8FC2B1F11C41703D8A9FFF1F5941'),
('0103000020110F000001000000050000008FC2F5C8C84F20413E0AD7A7585659418FC2F5C8C84F2041AE47E114D38159410AD7A3B0B8262241AE47E114D38159410AD7A3B0B82622413E0AD7A7585659418FC2F5C8C84F20413E0AD7A758565941'),
('0103000020110F0000010000000500000085EB5198A8FD2341EC51B860E9D3584185EB5198A8FD2341CDCCCC3ADE2A59410000008098D42541CDCCCC3ADE2A59410000008098D42541EC51B860E9D3584185EB5198A8FD2341EC51B860E9D35841'),
('0103000020110F000001000000050000008FC2F5C8C84F2041EC51B860E9D358418FC2F5C8C84F2041CDCCCC3ADE2A59410AD7A3B0B8262241CDCCCC3ADE2A59410AD7A3B0B8262241EC51B860E9D358418FC2F5C8C84F2041EC51B860E9D35841'),
('0103000020110F000001000000050000000AD7A3B0B826224114AE4704211559410AD7A3B0B82622413E0AD7A75856594185EB5198A8FD23413E0AD7A75856594185EB5198A8FD234114AE4704211559410AD7A3B0B826224114AE470421155941'),
('0103000020110F000001000000050000003E0AD723F29515417B14AEF36EA858413E0AD723F2951541EC51B860E9D358410000008098D42541EC51B860E9D358410000008098D425417B14AEF36EA858413E0AD723F29515417B14AEF36EA85841'),
('0103000020110F000001000000050000003E0AD723F2951541EC51B860E9D358413E0AD723F29515415C8FC2CD63FF5841343333F3D14319415C8FC2CD63FF5841343333F3D1431941EC51B860E9D358413E0AD723F2951541EC51B860E9D35841'),
('0103000020110F000001000000050000008FC2F5C8C84F2041CDCCCC3ADE2A59418FC2F5C8C84F20413E0AD7A7585659410AD7A3B0B82622413E0AD7A7585659410AD7A3B0B8262241CDCCCC3ADE2A59418FC2F5C8C84F2041CDCCCC3ADE2A5941'),
('0103000020110F00000100000005000000A4703DAAA1C81E415C8FC2CD63FF5841A4703DAAA1C81E41AE47E114D38159418FC2F5C8C84F2041AE47E114D38159418FC2F5C8C84F20415C8FC2CD63FF5841A4703DAAA1C81E415C8FC2CD63FF5841'),
('0103000020110F00000100000005000000295C8FC2B1F11C415C8FC2CD63FF5841295C8FC2B1F11C41703D8A9FFF1F5941A4703DAAA1C81E41703D8A9FFF1F5941A4703DAAA1C81E415C8FC2CD63FF5841295C8FC2B1F11C415C8FC2CD63FF5841');
SELECT create_reference_table('myshards');

INSERT INTO belgium_planet_osm_roads_geo SELECT s.id, b.k, b.osm_id, ST_Intersection(s.bbox, b.way) FROM belgium_planet_osm_roads_org b JOIN myshards s ON (s.bbox && b.way);
CREATE INDEX ON belgium_planet_osm_roads_geo USING gist (way);

UPDATE pg_dist_shard p SET shardcontainer = s.bbox::text, shardminvalue = NULL, shardmaxvalue = NULL FROM myshards s WHERE p.shardminvalue::integer = s.id AND p.logicalrelid = 'belgium_planet_osm_roads_geo'::regclass;
UPDATE pg_dist_partition
   SET partmethod = 'g'
     , colocationid = nextval('pg_dist_colocationid_seq'::regclass)
 WHERE logicalrelid = 'belgium_planet_osm_roads_geo'::regclass;

-- hack to get the correct partkey setup for the geo partition
BEGIN;
CREATE TABLE foo (LIKE belgium_planet_osm_roads_geo);
SELECT create_distributed_table('foo', 'way');
UPDATE pg_dist_partition t
   SET partkey = s.partkey
  FROM pg_dist_partition s
 WHERE s.logicalrelid = 'foo'::regclass
   AND t.logicalrelid = 'belgium_planet_osm_roads_geo'::regclass;
DROP TABLE foo;
COMMIT;
