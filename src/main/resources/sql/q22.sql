-- source
CREATE TABLE opc_registration(
    LINKMAN_ADDRESS VARCHAR
    ,PATIENT_NAME    VARCHAR
    ,LINKMAN_NAME    VARCHAR
    ,PERSON_INFO_ID  VARCHAR

) WITH (
  'connector.type' = 'kafka'
  ,'connector.version' = 'universal'
  ,'connector.topic' = 'opc_registration'                            -- required: topic name from which the table is read
  ,'connector.properties.zookeeper.connect' = 'slave1:2181'    -- required: specify the ZooKeeper connection string
  ,'connector.properties.bootstrap.servers' = 'slave2:9092'    -- required: specify the Kafka server connection string
  ,'connector.properties.group.id' = 'user_log'                   -- optional: required in Kafka consumer, specify consumer group
  ,'connector.startup-mode' = 'group-offsets'                     -- optional: valid modes are "earliest-offset", "latest-offset", "group-offsets",  "specific-offsets"
  ,'connector.sink-partitioner' = 'fixed'                        --optional fixed 每个 flink 分区数据只发到 一个 kafka 分区
  ,'format.type' = 'json'
);
-- sink mysql
CREATE TABLE pvuv_sink(
    dt VARCHAR,
    pv BIGINT,
    uv BIGINT
) WITH (
'connector' = 'jdbc'
,'url' = 'jdbc:mysql://master:3306/flink-test?charset=utf8'
,'table-name' = 'pvuv_sink'
,'username' = 'hive'
,'password' = '123456'
,'sink.buffer-flush.max-rows' = '100' -- default
,'sink.buffer-flush.interval' = '1s'
,'sink.max-retries' = '3'
);

INSERT INTO pvuv_sink
SELECT LINKMAN_ADDRESS AS dt,  20 AS pv, 30 AS uv
FROM opc_registration ;

-- INSERT INTO pvuv_sink
-- SELECT LINKMAN_ADDRESS AS dt,  COUNT(1) AS pv, COUNT(DISTINCT LINKMAN_ADDRESS) AS uv
-- FROM opc_registration GROUP BY LINKMAN_ADDRESS;


