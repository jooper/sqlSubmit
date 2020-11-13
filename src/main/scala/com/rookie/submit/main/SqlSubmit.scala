package com.rookie.submit.main

import java.io.File

import com.rookie.submit.common.{Common, Constant}
import com.rookie.submit.common.Constant._
import com.rookie.submit.util.{RegisterUdf, SqlFileUtil, TableConfUtil}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, StatementSet}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * sqlSubmit main class
  * input sql file name and execute sql content
  */
object SqlSubmit {

  private val logger = LoggerFactory.getLogger("SqlSubmit")

  def main(args: Array[String]): Unit = {
    // parse input parameter and load job properties
    val paraTool = Common.init(args)

    // parse sql file
    val sqlList = SqlFileUtil.readFile(paraTool.get(INPUT_SQL_FILE_PARA))

    // StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // state backend and checkpoint
    enableCheckpoint(env, paraTool)
    // EnvironmentSettings
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    // create table enviroment
    val tabEnv = StreamTableEnvironment.create(env, settings)
    tabEnv.getConfig.getConfiguration.setString("job.name", "ttttt")
    // table Config
    TableConfUtil.conf(tabEnv, paraTool)

    // register catalog, only in server
    //    if ("/".equals(File.separator)) {
    //      //      val catalog = new HiveCatalog(paraTool.get(Constant.HIVE_CATALOG_NAME), paraTool.get(Constant.HIVE_DEFAULT_DATABASE), paraTool.get(Constant.HIVE_CONFIG_PATH), paraTool.get(Constant.HIVE_VERSION))
    //      val catalog = new HiveCatalog(paraTool.get(Constant.HIVE_CATALOG_NAME), paraTool.get(Constant.HIVE_DEFAULT_DATABASE), paraTool.get(Constant.HIVE_CONFIG_PATH))
    //      tabEnv.registerCatalog(paraTool.get(Constant.HIVE_CATALOG_NAME), catalog)
    //      tabEnv.useCatalog(paraTool.get(Constant.HIVE_CATALOG_NAME))
    //    }

    // load udf
    RegisterUdf.registerUdf(tabEnv)

    // execute sql
    val statement = tabEnv.createStatementSet()
    var result: StatementSet = null
    for (sql <- sqlList) {
      //      val sql = sqlone.toLowerCase
      try {
        if (sql.toLowerCase.startsWith("insert")) {
          result = statement.addInsertSql(sql)
          //          tabEnv.executeSql(sql)
        } else {
          if (sql.contains("hive_table_")) {
            tabEnv.getConfig().setSqlDialect(SqlDialect.HIVE)
          } else {
            tabEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT)
          }
          logger.info("dialect : " + tabEnv.getConfig.getSqlDialect)
          println("dialect : " + tabEnv.getConfig.getSqlDialect)
          tabEnv.executeSql(sql)
        }
        logger.info("execute success : " + sql)
        println("execute success : " + sql)
      } catch {
        case e: Exception =>
          println("execute sql error : " + sql)
          logger.error("execute sql error : " + sql, e)
          e.printStackTrace()
          System.exit(-1)
      }
    }
    // execute insert
    //        result.execute(Common.jobName)
    result.execute()



    // not need, sql will execute when call executeSql
    //        env.execute(Common.jobName)

    //    env.execute("tttt")
  }

  def enableCheckpoint(env: StreamExecutionEnvironment, paraTool: ParameterTool): Unit = {
    // state backend
    var stateBackend: StateBackend = null
    if ("rocksdb".equals(paraTool.get(STATE_BACKEND))) {
      stateBackend = new RocksDBStateBackend(paraTool.get(CHECKPOINT_DIR), true)
    } else {
      stateBackend = new FsStateBackend(paraTool.get(CHECKPOINT_DIR), true)
    }
    env.setStateBackend(stateBackend)
    // checkpoint
    env.enableCheckpointing(paraTool.getLong(CHECKPOINT_INTERVAL) * 1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(paraTool.getLong(CHECKPOINT_TIMEOUT) * 1000)
    // Flink 1.11.0 new feature: Enables unaligned checkpoints
    env.getCheckpointConfig.enableUnalignedCheckpoints()
  }

}
