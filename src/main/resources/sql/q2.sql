-- source
CREATE TABLE opc_registration(
    LINKMAN_ADDRESS VARCHAR
    ,PATIENT_NAME    VARCHAR
    ,LINKMAN_NAME    VARCHAR
    ,PERSON_INFO_ID  VARCHAR

) WITH (
  'connector.type' = 'kafka'
  ,'connector.version' = 'universal'
  ,'connector.topic' = 'opc_registration'
  ,'connector.properties.zookeeper.connect' = 'slave1:2181'
  ,'connector.properties.bootstrap.servers' = 'slave2:9092'
  ,'connector.startup-mode' = 'latest-offset'   --latest=offset   earliest-offset
  ,'format.type' = 'json'
--   ,'properties.group.id' = 'pv_uv_1',
);



-- sink mysql
CREATE TABLE pvuv_sink(
    dt VARCHAR,
    pv BIGINT,
    uv BIGINT,
    PRIMARY KEY (dt) NOT ENFORCED
) WITH (
-- 'connector' = 'jdbc'
-- ,'url' = 'jdbc:mysql://master:3306/flink-test?characterEncoding=utf8'
-- ,'table-name' = 'pvuv_sink'
-- ,'username' = 'hive'
-- ,'password' = '123456'
-- ,'sink.buffer-flush.max-rows' = '100' -- default
-- ,'sink.buffer-flush.interval' = '1s'
-- ,'sink.max-retries' = '3'

'connector.type' = 'jdbc',
'connector.url' = 'jdbc:mysql://master:3306/flink-test?characterEncoding=utf-8',
'connector.table' = 'pvuv_sink',
'connector.username' = 'hive',
'connector.password' = '123456',
'connector.write.flush.max-rows' = '1'
-- ,'update-mode' = 'append'

--   'connector.type' = 'upsertKafka'
--   ,'connector.version' = 'universal's
--   ,'connector.topic' = 'pvuv_sink'
--   ,'connector.properties.zookeeper.connect' = 'slave2:2181'
--   ,'connector.properties.bootstrap.servers' = 'slave2:9092'
--   ,'format.type' = 'json'
);

INSERT INTO pvuv_sink
SELECT LINKMAN_ADDRESS AS dt,  COUNT(1) AS pv, COUNT(DISTINCT LINKMAN_ADDRESS) AS uv
FROM opc_registration
WHERE  LINKMAN_ADDRESS IS NOT NULL
GROUP BY LINKMAN_ADDRESS;

-- INSERT INTO pvuv_sink
-- SELECT LINKMAN_ADDRESS AS dt,  COUNT(1) AS pv, COUNT(DISTINCT LINKMAN_ADDRESS) AS uv
-- FROM opc_registration GROUP BY LINKMAN_ADDRESS;


