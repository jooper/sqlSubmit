package com.rookie.submit

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object kafka_upsert_sink {

  def main(args: Array[String]): Unit = {


    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val blinkStreamSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance
      .useBlinkPlanner
      .inStreamingMode.build

    val blinkStreamTableEnv = StreamTableEnvironment.create(senv, blinkStreamSettings)


    val source =
      """
        |CREATE TABLE opc_registration (
        |  DEPARTMENT_ID VARCHAR
        |) WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = 'universal',
        |    'connector.topic' = 'opc_registration',
        |    'connector.startup-mode' = 'latest-offset',
        |    'connector.properties.0.key' = 'zookeeper.connect',
        |    'connector.properties.0.value' = 'slave1:2181',
        |    'connector.properties.1.key' = 'bootstrap.servers',
        |    'connector.properties.1.value' = 'slave2:9092',
        |    'format.type' = 'json',
        |    'format.derive-schema' = 'true'
        |)
      """.stripMargin

    val sink =
      """
        |create table user_behavior_sink
        |(
        |department_id string
        |) WITH(
        |'connector.type' = 'upsertKafka',
        |'connector.version' = 'universal',
        |'connector.topic' = 'user_behavior_sink',
        |'connector.startup-mode' = 'latest-offset',
        |'connector.properties.0.key' = 'zookeeper.connect',
        |'connector.properties.0.value' = 'slave1:2181',
        |'connector.properties.1.key' = 'bootstrap.servers',
        |'connector.properties.1.value' = 'slave2:9092',
        |'format.type' = 'json',
        |'format.derive-schema' = 'true'
        |)
      """.stripMargin

    val sql = "insert into user_behavior_sink select DEPARTMENT_ID from opc_registration"





    //    val dim=
    //      """
    //        |CREATE TABLE hra00_department(
    //        |    ID varchar(10),
    //        |    DEPARTMENT_CHINESE_NAME varchar(100),
    //        |    PRIMARY KEY (ID),
    //        |    PERIOD FOR SYSTEM_TIME
    //        | )WITH(
    //        |    type='oracle',
    //        |    url = 'jdbc:oracle:thin:@10.158.5.84:1521:dbm',
    //        |    userName = 'ogg',
    //        |    password = 'ogg',
    //        |    tableName = 'hra00_department',
    //        |    cache = 'ALL',
    //        |    cacheTTLMs ='60000'
    //        | );
    //      """.stripMargin


    val dim =
      """
        |CREATE TABLE hra00_department(
        |    ID VARCHAR(10),
        |    DEPARTMENT_CHINESE_NAME VARCHAR(100)
        | )WITH(
        |    connector ='jdbc',
        |    url = 'jdbc:oracle:thin:@10.158.5.84:1521:dbm',
        |    username = 'ogg',
        |    password = 'ogg',
        |    table-name = 'hra00_department'
        | );
      """.stripMargin



    //    blinkStreamTableEnv.get().sqlUpdate(dim)
    blinkStreamTableEnv.executeSql(source)
    blinkStreamTableEnv.executeSql(sink)
    blinkStreamTableEnv.executeSql(sql)
    blinkStreamTableEnv.execute("kafka sink upsert")

  }
}
