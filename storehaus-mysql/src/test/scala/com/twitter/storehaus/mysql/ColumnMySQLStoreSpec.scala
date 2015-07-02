package com.twitter.storehaus.mysql

import com.twitter.finagle.exp.mysql.{Client => MySQLClient, _}
import com.twitter.util.{Await, Future}
import org.specs2.mock.Mockito
import org.mockito.Mockito._
import org.specs2.mutable.Specification

import scala.collection.mutable

/**
 * Created by s.djamaa on 26/05/2015.
 */
class ColumnMySQLStoreSpec extends Specification with Mockito {
  "ColumnMySQLStore" should {

    "create a SELECT query" in {

      val mysqlClient = mock[MySQLClient]

      val selectSql = "SELECT `val1`,`val2` FROM `myTable` WHERE `key1` =? AND `key2` =?"

      when(mysqlClient.prepare(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 2, 0, Seq.empty[Field], Seq.empty[Field]))))
      when(mysqlClient.select(any[PreparedStatement])(anyFunction1[Row, ColumnMySqlValue])).thenCallRealMethod

      val row1 = new Row {
        override val fields: IndexedSeq[Field] = null

        override def indexOf(columnName: String): Option[Int] = indexMap.get(columnName)

        override val values: IndexedSeq[Value] = IndexedSeq(IntValue(10), IntValue(20))

        val indexMap = Map("val1" -> 0, "val2" -> 1)
      }

      when(mysqlClient.execute(any[PreparedStatement])).thenReturn(Future.value(ResultSet(null, Seq(row1))))

      val store = new ColumnMySQLStore(mysqlClient, "myTable", List("key1", "key2"), List("val1", "val2"))

      val keys = new ColumnMySqlValue(List(
        MySqlValue(IntValue(1)),
        MySqlValue(IntValue(2))
      ))

      val res = store.get(keys)

      Await.result(res).get.v must containTheSameElementsAs(List(MySqlValue(IntValue(10)), MySqlValue(IntValue(20))))

      val selectStatement = capture[PreparedStatement]
      verify(mysqlClient).select(selectStatement.capture)(anyFunction1[Row, ColumnMySqlValue])

      store.SELECT_SQL mustEqual selectSql

      val actualParameters = selectStatement.value.parameters.toSeq.map { case bytes: Array[Byte] => new String(bytes)}

      actualParameters must containTheSameElementsAs(Seq("1", "2"))
    }

    "create an UPDATE query" in {

      val mysqlClient = mock[MySQLClient]

      val updateSql = "UPDATE `myTable` SET `val1` =?,`val2` =? WHERE `key1` =? AND `key2` =?"

      val keys = new ColumnMySqlValue(List(
        MySqlValue(IntValue(1)),
        MySqlValue(IntValue(2))
      ))

      val values = Some(new ColumnMySqlValue(List(
        MySqlValue(IntValue(20)),
        MySqlValue(IntValue(10))
      )))

      when(mysqlClient.prepare(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 4, 0, Seq.empty[Field], Seq.empty[Field]))))
      when(mysqlClient.execute(any[PreparedStatement])).thenReturn(Future.value(CloseStatementOK))

      val store = spy(new ColumnMySQLStore(mysqlClient, "myTable", List("key1", "key2"), List("val1", "val2")))

      doReturn(Future.value(Some(values))).when(store).get(any[ColumnMySqlValue])

      store.put(keys, values)

      val updateStatement = capture[PreparedStatement]
      verify(mysqlClient).execute(updateStatement.capture)

      store.UPDATE_SQL mustEqual updateSql

      val actualParameters = updateStatement.value.parameters.toSeq.map { case bytes: Array[Byte] => new String(bytes)}

      actualParameters must containTheSameElementsAs(Seq("20", "10", "1", "2"))
    }

    "create an INSERT query" in {

      val mysqlClient = mock[MySQLClient]

      val insertSql = "INSERT INTO `myTable` (`key1`,`key2`,`val1`,`val2`) VALUES (?,?,?,?)"

      val keys = new ColumnMySqlValue(List(
        MySqlValue(IntValue(1)),
        MySqlValue(IntValue(2))
      ))

      val values = Some(new ColumnMySqlValue(List(
        MySqlValue(IntValue(20)),
        MySqlValue(IntValue(10))
      )))

      when(mysqlClient.prepare(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 4, 0, Seq.empty[Field], Seq.empty[Field]))))
      when(mysqlClient.execute(any[PreparedStatement])).thenReturn(Future.value(CloseStatementOK))

      val store = spy(new ColumnMySQLStore(mysqlClient, "myTable", List("key1", "key2"), List("val1", "val2")))

      doReturn(Future.value(None)).when(store).get(any[ColumnMySqlValue])

      store.put(keys, values)

      val insertStatement = capture[PreparedStatement]
      verify(mysqlClient).execute(insertStatement.capture)

      store.INSERT_SQL mustEqual insertSql

      val actualParameters = insertStatement.value.parameters.toSeq.map { case bytes: Array[Byte] => new String(bytes)}

      actualParameters must containTheSameElementsAs(Seq("1", "2", "20", "10"))
    }

    "create a DELETE query" in {

      val mysqlClient = mock[MySQLClient]

      val deleteSql = "DELETE FROM `myTable` WHERE `key1` =? AND `key2` =?"

      val keys = new ColumnMySqlValue(List(
        MySqlValue(IntValue(1)),
        MySqlValue(IntValue(2))
      ))

      val values = None

      when(mysqlClient.prepare(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 2, 0, Seq.empty[Field], Seq.empty[Field]))))
      when(mysqlClient.execute(any[PreparedStatement])).thenReturn(Future.value(CloseStatementOK))

      val store = spy(new ColumnMySQLStore(mysqlClient, "myTable", List("key1", "key2"), List("val1", "val2")))

      doReturn(Future.value(None)).when(store).get(any[ColumnMySqlValue])

      store.put(keys, values)

      val deleteStatement = capture[PreparedStatement]
      verify(mysqlClient).execute(deleteStatement.capture)

      store.DELETE_SQL mustEqual deleteSql

      val actualParameters = deleteStatement.value.parameters.toSeq.map { case bytes: Array[Byte] => new String(bytes)}

      actualParameters must containTheSameElementsAs(Seq("1", "2"))
    }

    "create a MULTI SELECT query" in {

      val mysqlClient = mock[MySQLClient]

      val expectedSelectSql = "SELECT `key1`,`key2`,`val1`,`val2` FROM `myTable` WHERE (`key1`,`key2`) IN ((?,?),(?,?),(?,?))"

      when(mysqlClient.prepare(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 2, 0, Seq.empty[Field], Seq.empty[Field]))))

      when(mysqlClient.prepareAndSelect(anyString, any[Array[Byte]])(anyFunction1[Row, ColumnMySqlValue]))
        .thenReturn(Future.value(
        new PreparedStatement(new PrepareOK(0, 0, 6, 0, Seq.empty[Field], Seq.empty[Field])),
        Seq.empty[ColumnMySqlValue]))

      val store = new ColumnMySQLStore(mysqlClient, "myTable", List("key1", "key2"), List("val1", "val2"))

      val keys = Set(
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(1)),
          MySqlValue(IntValue(2))
        )),
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(100)),
          MySqlValue(IntValue(200))
        )),
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(1000)),
          MySqlValue(IntValue(2000))
        )))

      store.multiGet(keys)

      val multiSelectStatement = capture[String]

      verify(mysqlClient).prepareAndSelect(multiSelectStatement.capture, any[Array[Any]])(anyFunction1[Row, ColumnMySqlValue])

      multiSelectStatement.value mustEqual expectedSelectSql

      val actualSqlParams = store.serializeMySqlValue(keys).map { new String(_) }.toSeq

      actualSqlParams must containTheSameElementsAs (Seq("1", "2", "100", "200", "1000", "2000"))
    }

    "create a MULTI INSERT query" in {

      val mysqlClient = mock[MySQLClient]

      val insertSql = "INSERT INTO `myTable` (`key1`,`key2`,`val1`,`val2`) VALUES (?,?,?,?),(?,?,?,?),(?,?,?,?)"

      val kvsOpt = Map(
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(1)),
          MySqlValue(IntValue(2))
        )) -> Some(new ColumnMySqlValue(List(
          MySqlValue(IntValue(100)),
          MySqlValue(IntValue(200))
        ))),
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(3)),
          MySqlValue(IntValue(4))
        )) -> Some(new ColumnMySqlValue(List(
          MySqlValue(IntValue(1000)),
          MySqlValue(IntValue(2000))
        ))),
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(5)),
          MySqlValue(IntValue(6))
        )) -> Some(new ColumnMySqlValue(List(
          MySqlValue(IntValue(10)),
          MySqlValue(IntValue(20))
        )))
      )

      val kvs = kvsOpt.mapValues { v => v.get }

      // Start transaction, commit & rollback
      when(mysqlClient.query(anyString)).thenReturn(Future.value(CloseStatementOK))
      // All prepared statements (select, update...)
      when(mysqlClient.prepare(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 12, 0, Seq.empty[Field], Seq.empty[Field]))))
      //
      when(mysqlClient.execute(any[PreparedStatement])).thenReturn(Future.value(CloseStatementOK))
      when(mysqlClient.closeStatement(any[PreparedStatement])).thenReturn(Future.value(CloseStatementOK))

      val store = spy(new ColumnMySQLStore(mysqlClient, "myTable", List("key1", "key2"), List("val1", "val2")))

      // None of the keys exist in database
      doReturn(kvs.mapValues{ k => Future.value(None) }).when(store).multiGet(kvsOpt.keySet)
      //
      when(mysqlClient.prepareAndExecute(anyString, any[Array[Any]])).thenReturn(Future.value((null, null)))

      store.multiPut(kvsOpt)

      val multiInsertStatement = capture[String]
      verify(mysqlClient).prepareAndExecute(multiInsertStatement.capture, any[Array[Any]])

      multiInsertStatement.value mustEqual insertSql

      val actualParameters = store.serializeValues(kvs).map { new String(_) }.toSeq

      actualParameters must containTheSameElementsAs(Seq("1", "2", "100", "200", "3", "4", "1000", "2000", "5", "6", "10", "20"))
    }

    "create a MULTI DELETE query" in {

      val mysqlClient = mock[MySQLClient]

      val deleteSql = "DELETE FROM `myTable` WHERE (`key1`,`key2`) IN ((?,?),(?,?),(?,?))"

      val kvsInDatabase = Map(
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(1)),
          MySqlValue(IntValue(2))
        )) -> new ColumnMySqlValue(List(
          MySqlValue(IntValue(100)),
          MySqlValue(IntValue(200))
        )),
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(3)),
          MySqlValue(IntValue(4))
        )) -> new ColumnMySqlValue(List(
          MySqlValue(IntValue(1000)),
          MySqlValue(IntValue(2000))
        )),
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(5)),
          MySqlValue(IntValue(6))
        )) -> new ColumnMySqlValue(List(
          MySqlValue(IntValue(10)),
          MySqlValue(IntValue(20))
        ))
      )

      val kvsFromArgs = kvsInDatabase.mapValues { v => None }

      // Start transaction, commit & rollback
      when(mysqlClient.query(anyString)).thenReturn(Future.value(CloseStatementOK))
      // All prepared statements (select, update...)
      when(mysqlClient.prepare(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 6, 0, Seq.empty[Field], Seq.empty[Field]))))
      //
      when(mysqlClient.execute(any[PreparedStatement])).thenReturn(Future.value(CloseStatementOK))
      when(mysqlClient.closeStatement(any[PreparedStatement])).thenReturn(Future.value(CloseStatementOK))

      val store = spy(new ColumnMySQLStore(mysqlClient, "myTable", List("key1", "key2"), List("val1", "val2")))

      // None of the keys exist in database
      doReturn(kvsInDatabase.mapValues{ v => Future.value(Some(v)) }).when(store).multiGet(kvsFromArgs.keySet)
      //
      when(mysqlClient.prepareAndExecute(anyString, any[Array[Any]])).thenReturn(Future.value((null, null)))

      store.multiPut(kvsFromArgs)

      val multiDeleteStatement = capture[String]
      verify(mysqlClient).prepareAndExecute(multiDeleteStatement.capture, any[Array[Any]])

      multiDeleteStatement.value mustEqual deleteSql

      val actualParameters = store.serializeKeys(kvsInDatabase.keySet).map { new String(_) }.toSeq

      actualParameters must containTheSameElementsAs(Seq("1", "2", "3", "4", "5", "6"))
    }

    "create a MULTI UPDATE query" in {

      val mysqlClient = mock[MySQLClient]

      val deleteSql = "UPDATE `myTable` " +
        "SET `val1` = CASE" +
        " WHEN `key1` =? AND `key2` =? THEN ?" +
        " WHEN `key1` =? AND `key2` =? THEN ?" +
        " WHEN `key1` =? AND `key2` =? THEN ?" +
        " END," +
        " SET `val2` = CASE" +
        " WHEN `key1` =? AND `key2` =? THEN ?" +
        " WHEN `key1` =? AND `key2` =? THEN ?" +
        " WHEN `key1` =? AND `key2` =? THEN ?" +
        " END" +
        " WHERE (`key1`,`key2`) IN ((?,?),(?,?),(?,?))"

      val kvsInDatabase = Map(
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(1)),
          MySqlValue(IntValue(2))
        )) -> new ColumnMySqlValue(List(
          MySqlValue(IntValue(100)),
          MySqlValue(IntValue(200))
        )),
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(3)),
          MySqlValue(IntValue(4))
        )) -> new ColumnMySqlValue(List(
          MySqlValue(IntValue(1000)),
          MySqlValue(IntValue(2000))
        )),
        new ColumnMySqlValue(List(
          MySqlValue(IntValue(5)),
          MySqlValue(IntValue(6))
        )) -> new ColumnMySqlValue(List(
          MySqlValue(IntValue(10)),
          MySqlValue(IntValue(20))
        ))
      )

      val kvsFromArgs = kvsInDatabase.mapValues { v => Some(v) }

      // Start transaction, commit & rollback
      when(mysqlClient.query(anyString)).thenReturn(Future.value(CloseStatementOK))
      // All prepared statements (select, update...)
      when(mysqlClient.prepare(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 24, 0, Seq.empty[Field], Seq.empty[Field]))))
      //
      when(mysqlClient.execute(any[PreparedStatement])).thenReturn(Future.value(CloseStatementOK))
      when(mysqlClient.closeStatement(any[PreparedStatement])).thenReturn(Future.value(CloseStatementOK))

      val store = spy(new ColumnMySQLStore(mysqlClient, "myTable", List("key1", "key2"), List("val1", "val2")))

      // None of the keys exist in database
      doReturn(kvsInDatabase.mapValues{ v => Future.value(Some(v)) }).when(store).multiGet(kvsFromArgs.keySet)
      //
      when(mysqlClient.prepareAndExecute(anyString, any[Array[Any]])).thenReturn(Future.value((null, null)))

      store.multiPut(kvsFromArgs)

      val multiDeleteStatement = capture[String]
      verify(mysqlClient).prepareAndExecute(multiDeleteStatement.capture, any[Array[Any]])

      multiDeleteStatement.value mustEqual deleteSql

      val actualParameters = store.serializeKeyAndValueForUpdate(kvsFromArgs.mapValues(_.get)).map { new String(_) }.toSeq

      actualParameters must containTheSameElementsAs(Seq("1", "2", "100", "3", "4", "1000", "5", "6", "10", "1", "2", "200", "3", "4", "2000", "5", "6", "20", "1", "2", "3", "4", "5", "6"))
    }
  }
}
