package com.rookie.submit.main


import java.util

import com.rookie.submit.common.Common
import com.rookie.submit.common.Constant._
import com.rookie.submit.util.{RegisterUdf, SqlFileUtil, TableConfUtil}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object SubmitSqlJob {
  private val logger = LoggerFactory.getLogger("SqlSubmit")

  def main(args: Array[String]): Unit = {

    val paraTool = Common.init(args)
    val (sqlList: util.List[String], env: StreamExecutionEnvironment, tEnv: StreamTableEnvironment) = InitEnv(paraTool)
    excuteSql(sqlList, tEnv)
  }

  private def excuteSql(sqlList: util.List[String], tEnv: StreamTableEnvironment) = {
    for (sql <- sqlList) {
      try {
        if (sql.toLowerCase.startsWith("insert")) {
          tEnv.sqlUpdate(sql)
        } else {
          setDialect(tEnv, sql)
          tEnv.sqlUpdate(sql)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          System.exit(-1)
      }
    }
    tEnv.execute(Common.jobName) //job名称为对应的  sql文件名
  }

  private def setDialect(tEnv: StreamTableEnvironment, sql: String) = {
    if (sql.contains("hive_table_")) {
      tEnv.getConfig().setSqlDialect(SqlDialect.HIVE)
    } else {
      tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT)
    }
  }

  private def InitEnv(paraTool: ParameterTool) = {
    val sqlList = SqlFileUtil.readFile(paraTool.get(INPUT_SQL_FILE_PARA))
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    enableCheckpoint(env, paraTool)
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    //    tEnv.getConfig.getConfiguration.setString("job.name", "OdsCanalFcboxSendIngressStream")
    TableConfUtil.conf(tEnv, paraTool)
    RegisterUdf.registerUdf(tEnv)
    (sqlList, env, tEnv)
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
