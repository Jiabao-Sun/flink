/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.api.common.BatchShuffleMode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.{BatchExecutionOptions, ExecutionOptions, JobManagerOptions}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.writer.BinaryRowWriter
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.runtime.utils.BatchAbstractTestBase.DEFAULT_PARALLELISM
import org.apache.flink.table.planner.runtime.utils.BatchTestBase._
import org.apache.flink.table.planner.utils.{TableTestUtil, TestingTableEnvironment}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.runtime.util.RowDataTestUtil
import org.apache.flink.table.types.logical.{BigIntType, LogicalType}
import org.apache.flink.types.Row
import org.apache.flink.util.CollectionUtil

import _root_.java.lang.{Iterable => JIterable}
import _root_.java.util.regex.Pattern
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable.ArrayBuffer
import _root_.scala.util.Sorting
import org.apache.calcite.rel.RelNode
import org.apache.calcite.runtime.CalciteContextException
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql.parser.SqlParseException
import org.assertj.core.api.Assertions.fail
import org.junit.jupiter.api.{AfterEach, BeforeEach}

class BatchTestBase extends BatchAbstractTestBase {

  protected var settings = EnvironmentSettings.newInstance().inBatchMode().build()
  protected var testingTableEnv: TestingTableEnvironment = _
  protected var tEnv: TableEnvironment = _
  protected var planner: PlannerBase = _
  protected var env: StreamExecutionEnvironment = _
  protected var tableConfig: TableConfig = _

  val LINE_COL_PATTERN: Pattern = Pattern.compile("At line ([0-9]+), column ([0-9]+)")
  val LINE_COL_TWICE_PATTERN: Pattern = Pattern.compile(
    "(?s)From line ([0-9]+),"
      + " column ([0-9]+) to line ([0-9]+), column ([0-9]+): (.*)")

  @throws(classOf[Exception])
  @BeforeEach
  def setupEnv(): Unit = {
    testingTableEnv = TestingTableEnvironment
      .create(settings, catalogManager = None, TableConfig.getDefault)
    tEnv = testingTableEnv
    tEnv.getConfig.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, Boolean.box(false))
    planner = tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
    env = planner.getExecEnv
    env.getConfig.enableObjectReuse()
    tableConfig = tEnv.getConfig
    BatchTestBase.configForMiniCluster(tableConfig)
  }

  @throws(classOf[Exception])
  @BeforeEach
  def before(): Unit = {}

  @AfterEach
  def after(): Unit = {
    TestValuesTableFactory.clearAllData()
  }

  def checkResult(sqlQuery: String, expectedResult: Seq[Row], isSorted: Boolean = false): Unit = {
    check(sqlQuery, (result: Seq[Row]) => checkSame(expectedResult, result, isSorted), tEnv)
  }

  def checkTableResult(table: Table, expectedResult: Seq[Row], isSorted: Boolean = false): Unit = {
    checkTable(table, (result: Seq[Row]) => checkSame(expectedResult, result, isSorted))
  }

  def checkSize(sqlQuery: String, expectedSize: Int): Unit = {
    check(
      sqlQuery,
      (result: Seq[Row]) => {
        if (result.size != expectedSize) {
          val errorMessage =
            s"""
               |Results
               |${sideBySide(
                s"== Correct Result - $expectedSize ==" +:
                  prepareResult(Seq(), isSorted = false),
                s"== Actual Result - ${result.size} ==" +:
                  prepareResult(result, isSorted = false)
              ).mkString("\n")}
        """.stripMargin
          Some(errorMessage)
        } else None
      },
      tEnv
    )
  }

  def checkTable(table: Table, checkFunc: Seq[Row] => Option[String]): Unit = {
    val result = executeQuery(table)

    checkFunc(result).foreach {
      results =>
        val plan = explainLogical(table, tEnv)
        fail(s"""
                |Results do not match:
                |$results
                |Plan:
                |  $plan
       """.stripMargin)
    }
  }

  def checkFailed(sqlQuery: String, expectedMsgPattern: String): Unit = {
    try {
      val table = parseQuery(sqlQuery)
      val _ = executeQuery(table)
      // If got here, no exception is thrown.
      if (expectedMsgPattern != null) {
        throw new AssertionError(
          s"Expected query to throw exception, but it did not;"
            + s" query [$sqlQuery ];"
            + s" expected [$expectedMsgPattern]")
      }
    } catch {
      case spe: SqlParserException =>
        val errMsg = spe.getMessage
        if (null == expectedMsgPattern) {
          throw new RuntimeException(s"Error while parsing query: $sqlQuery", spe)
        } else if (null == errMsg || !errMsg.matches(expectedMsgPattern)) {
          throw new RuntimeException(
            s"Error did not match expected [$expectedMsgPattern] while "
              + s"parsing query [$sqlQuery]",
            spe)
        }
      case thrown: Throwable =>
        var actualExp = thrown
        var actualMsg = actualExp.getMessage
        var actualLine = -1
        var actualColumn = -1
        var actualEndLine = 100
        var actualEndColumn = 99

        var ece: CalciteContextException = null
        var spe: SqlParseException = null
        var ex = actualExp
        var found = false
        while (null != ex && !found) {
          ex match {
            case ex1: CalciteContextException =>
              ece = ex1
              found = true
            case ex2: Throwable if ex2.getCause == ex =>
              found = true
            case _ =>
              ex = ex.getCause
          }
        }
        ex = actualExp
        found = false
        while (null != ex && !found) {
          ex match {
            case ex1: SqlParseException if ex1.getPos != null =>
              spe = ex1
              found = true
            case ex2: Throwable if ex2.getCause == ex =>
              found = true
            case _ =>
              ex = ex.getCause
          }
        }

        if (ece != null) {
          actualLine = ece.getPosLine
          actualColumn = ece.getPosColumn
          actualEndLine = ece.getEndPosLine
          actualEndColumn = ece.getEndPosColumn
          if (ece.getCause != null) {
            actualExp = ece.getCause
            actualMsg = actualExp.getMessage
          }
        } else if (spe != null) {
          actualLine = spe.getPos.getLineNum
          actualColumn = spe.getPos.getColumnNum
          actualEndLine = spe.getPos.getEndLineNum
          actualEndColumn = spe.getPos.getEndColumnNum
          if (spe.getCause != null) {
            actualExp = spe.getCause
            actualMsg = actualExp.getMessage
          }
        } else {
          val message = actualMsg
          if (message != null) {
            var matcher = LINE_COL_TWICE_PATTERN.matcher(message)
            if (matcher.matches) {
              actualLine = matcher.group(1).toInt
              actualColumn = matcher.group(2).toInt
              actualEndLine = matcher.group(3).toInt
              actualEndColumn = matcher.group(4).toInt
              actualMsg = matcher.group(5)
            } else {
              matcher = LINE_COL_PATTERN.matcher(message)
              if (matcher.matches) {
                actualLine = matcher.group(1).toInt
                actualColumn = matcher.group(2).toInt
              }
            }
          }
        }
        if (null == expectedMsgPattern) {
          actualExp.printStackTrace()
          fail(
            s"Validator threw unexpected exception; query [$sqlQuery];" +
              s" exception [$actualMsg]; class [$actualExp.getClass];" +
              s" pos [line $actualLine col $actualColumn thru line $actualLine col $actualColumn]")
        }
        if (actualMsg == null || !actualMsg.matches(expectedMsgPattern)) {
          actualExp.printStackTrace()
          fail(
            s"Validator threw different "
              + s"exception than expected; query [$sqlQuery];\n"
              + s" expected pattern [$expectedMsgPattern];\n"
              + s" actual [$actualMsg];\n"
              + s" pos [$actualLine"
              + s" col $actualColumn"
              + s" thru line $actualEndLine"
              + s" col $actualEndColumn].")
        }
    }
  }

  def checkEmptyResult(sqlQuery: String): Unit = {
    val table = parseQuery(sqlQuery)
    val result = executeQuery(table)

    checkEmpty(result).foreach {
      results =>
        fail(s"""
                |Results do not match for query:
                |$results
       """.stripMargin)
    }
  }

  def parseQuery(sqlQuery: String): Table = {
    BatchTestBase.parseQuery(sqlQuery, tEnv)
  }

  def executeQuery(table: Table): Seq[Row] = {
    BatchTestBase.executeQuery(table)
  }

  def executeQuery(sqlQuery: String): Seq[Row] = {
    val table = parseQuery(sqlQuery)
    executeQuery(table)
  }

  private def prepareResult(seq: Seq[Row], isSorted: Boolean): Seq[String] = {
    if (!isSorted) seq.map(_.toString).sortBy(s => s) else seq.map(_.toString)
  }

  private def checkEmpty(result: Seq[Row]) = {
    val expectedResult = Nil
    checkSame(expectedResult, result, isSorted = true)
  }

  implicit def registerCollection(
      tableName: String,
      data: Iterable[Row],
      typeInfo: TypeInformation[Row],
      fields: String): Unit = {

    BatchTableEnvUtil.registerCollection(tEnv, tableName, data, typeInfo, fields)
  }

  def registerJavaCollection[T](
      tableName: String,
      data: JIterable[T],
      typeInfo: TypeInformation[T],
      fields: String): Unit = {
    BatchTableEnvUtil.registerCollection(tEnv, tableName, data.asScala, typeInfo, fields)
  }

  def registerCollection[T](
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fields: String,
      fieldNullables: Array[Boolean]): Unit = {
    BatchTableEnvUtil.registerCollection(
      tEnv,
      tableName,
      data,
      typeInfo,
      fields,
      fieldNullables,
      None)
  }

  def registerCollection[T](
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fields: String,
      fieldNullables: Array[Boolean],
      statistic: FlinkStatistic): Unit = {
    BatchTableEnvUtil.registerCollection(
      tEnv,
      tableName,
      data,
      typeInfo,
      fields,
      fieldNullables,
      Some(statistic))
  }

  def registerTemporarySystemFunction(
      name: String,
      functionClass: Class[_ <: UserDefinedFunction]): Unit = {
    testingTableEnv.createTemporarySystemFunction(name, functionClass)
  }

  def registerRange(name: String, end: Long): Unit = {
    registerRange(name, 0, end)
  }

  def registerRange(name: String, start: Long, end: Long): Unit = {
    BatchTableEnvUtil.registerBoundedStreamInternal(
      tEnv,
      name,
      newRangeSource(start, end),
      Some(Array($"id")),
      None,
      None)
  }

  def newRangeSource(start: Long, end: Long): DataStream[RowData] = {
    val typeInfo: TypeInformation[RowData] = InternalTypeInfo.ofFields(new BigIntType)
    val boundedStream = env.createInput(new RangeInputFormat(start, end), typeInfo)
    boundedStream.setParallelism(1)
    boundedStream
  }

  /**
   * Creates a new Row and assigns the given values to the Row's fields. We use [[rowOf()]] here to
   * avoid conflicts with [[ImplicitExpressionConversions.row]].
   */
  protected def rowOf(args: Any*): Row = {
    val row = new Row(args.length)
    (0 until args.length).foreach(i => row.setField(i, args(i)))
    row
  }
}

object BatchTestBase {

  def row(args: Any*): Row = {
    val values = args.toArray
    val row = new Row(values.length)
    var i = 0
    while (i < values.length) {
      row.setField(i, values(i))
      i += 1
    }
    row
  }

  def binaryRow(types: Array[LogicalType], fields: Any*): BinaryRowData = {
    // TODO, replace the failure check with a new and simpler checking method
    if (fields.length != types.length) {
      fail("Filed count inconsistent with type information")
    }
    val row = new BinaryRowData(fields.length)
    val writer = new BinaryRowWriter(row)
    writer.reset()
    fields.zipWithIndex.foreach {
      case (field, index) =>
        if (field == null) writer.setNullAt(index)
        else RowDataTestUtil.write(writer, index, field, types(index))
    }
    writer.complete()
    row
  }

  def compareResult[T](
      expectedStrings: Array[String],
      result: Array[T],
      sort: Boolean,
      asTuples: Boolean = false): Unit = {
    val resultStringsBuffer: ArrayBuffer[String] = new ArrayBuffer[String](result.length)
    result.foreach {
      v =>
        v match {
          case t0: Tuple if asTuples =>
            val first: Any = t0.getField(0)
            val bld: StringBuilder =
              new StringBuilder(if (first == null) "null" else first.toString)
            (1 until t0.getArity).foreach {
              idx =>
                val next = t0.getField(idx)
                bld.append(',').append(if (next == null) "null" else next.toString)
                resultStringsBuffer += bld.toString()
            }
          case _ =>
        }
        if (asTuples && !v.isInstanceOf[Tuple]) {
          throw new IllegalArgumentException(v + " is no tuple")
        }
        if (!asTuples) {
          resultStringsBuffer += (if (v == null) "null" else v.toString)
        }
    }
    val resultStrings = resultStringsBuffer.toArray
    if (sort) {
      Sorting.quickSort(expectedStrings)
      Sorting.quickSort(resultStrings)
    }
    val msg = s"Different elements in arrays: expected ${expectedStrings.length} elements " +
      s"and received ${resultStrings.length}\n " +
      s"expected: ${expectedStrings.mkString}\n " +
      s"received: ${resultStrings.mkString}"
    // TODO, replace these two failure checks with new and simpler checking methods
    if (expectedStrings.length != resultStrings.length) {
      fail(msg)
    }
    expectedStrings.zip(resultStrings).foreach {
      case (e, r) =>
        if (e != r) {
          fail(msg)
        }
    }
  }

  def configForMiniCluster(tableConfig: TableConfig): Unit = {
    tableConfig.set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(DEFAULT_PARALLELISM))
  }

  def configBatchShuffleMode(tableConfig: TableConfig, shuffleMode: BatchShuffleMode): Unit = {
    tableConfig.set(ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode)
    if (shuffleMode == BatchShuffleMode.ALL_EXCHANGES_PIPELINED) {
      tableConfig.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Default)
    } else {
      tableConfig.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch)
    }
  }

  def checkResult(
      sqlQuery: String,
      expectedResult: Seq[Row],
      tEnv: TableEnvironment,
      isSorted: Boolean = false): Unit = {
    check(sqlQuery, (result: Seq[Row]) => checkSame(expectedResult, result, isSorted), tEnv)
  }

  def check(
      sqlQuery: String,
      checkFunc: Seq[Row] => Option[String],
      tEnv: TableEnvironment): Unit = {
    val table = parseQuery(sqlQuery, tEnv)
    val result = executeQuery(table)

    checkFunc(result).foreach {
      results =>
        val plan = explainLogical(table, tEnv)
        fail(s"""
                |Results do not match for query:
                |  $sqlQuery
                |$results
                |Plan:
                |  $plan
       """.stripMargin)
    }
  }

  def parseQuery(sqlQuery: String, tEnv: TableEnvironment): Table = tEnv.sqlQuery(sqlQuery)

  def executeQuery(table: Table): Seq[Row] = {
    CollectionUtil.iteratorToList(table.execute().collect()).asScala
  }

  def executeQuery(sqlQuery: String, tEnv: TableEnvironment): Seq[Row] = {
    val table = parseQuery(sqlQuery, tEnv)
    executeQuery(table)
  }

  /**
   * Explain ast tree nodes of table and the logical plan after optimization.
   *
   * @param table
   *   table to explain for
   * @param tEnv
   *   table environment
   * @return
   *   string presentation of explaining
   */
  def explainLogical(table: Table, tEnv: TableEnvironment): String = {
    val ast = TableTestUtil.toRelNode(table)
    val logicalPlan = getPlan(ast, tEnv)

    s"== Abstract Syntax Tree ==" +
      System.lineSeparator +
      s"${FlinkRelOptUtil.toString(ast)}" +
      System.lineSeparator +
      s"== Optimized Logical Plan ==" +
      System.lineSeparator +
      s"$logicalPlan"
  }

  private def getPlan(relNode: RelNode, tEnv: TableEnvironment): String = {
    val planner = tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
    val optimized = planner.optimize(relNode)
    FlinkRelOptUtil.toString(optimized, SqlExplainLevel.EXPPLAN_ATTRIBUTES)
  }

  def checkSame(
      expectedResult: Seq[Row],
      result: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    if (
      expectedResult.size != result.size
      || !prepareResult(expectedResult, isSorted).equals(prepareResult(result, isSorted))
    ) {
      val errorMessage =
        s"""
           |Results
           |${sideBySide(
            s"== Correct Result - ${expectedResult.size} ==" +:
              prepareResult(expectedResult, isSorted),
            s"== Actual Result - ${result.size} ==" +:
              prepareResult(result, isSorted)
          ).mkString("\n")}
        """.stripMargin
      Some(errorMessage)
    } else None
  }

  private def prepareResult(seq: Seq[Row], isSorted: Boolean): Seq[String] = {
    if (!isSorted) seq.map(_.toString).sortBy(s => s) else seq.map(_.toString)
  }

  private def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.length).max
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

    leftPadded.zip(rightPadded).map {
      case (l, r) =>
        (if (l == r || l.startsWith("== Correct")) " " else "!") +
          l + (" " * ((maxLeftSize - l.length) + 3)) + r
    }
  }
}
