package com.twitter.storehaus.mysql

import com.twitter.finagle.exp.mysql.{Client => MySQLClient, _}
import com.twitter.util.Future
import org.specs2.mock.Mockito
import org.mockito.Mockito._
import org.specs2.mutable.Specification

/**
 * Created by s.djamaa on 26/05/2015.
 */
class ColumnMySQLStoreSpec extends Specification with Mockito {
  "ColumnMySQLStore" should {

    "create a SELECT query" in {

      val mysqlClient = mock[MySQLClient]

      val selectSql = "SELECT `val1`,`val2` FROM `myTable` WHERE `key1` =? AND `key2` =?"

      when(mysqlClient.prepare(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 2, 0, Seq.empty[Field], Seq.empty[Field]))))
      when(mysqlClient.select(any[PreparedStatement])(anyFunction1[Row, ColumnMySqlValue])).thenReturn(Future.value(Seq.empty[ColumnMySqlValue]))

      val store = new ColumnMySQLStore(mysqlClient, "myTable", List("key1", "key2"), List("val1", "val2"))

      val keys = new ColumnMySqlValue(List(
        MySqlValue(IntValue(1)),
        MySqlValue(IntValue(2))
      ))

      store.get(keys)

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

      val selectSql = "SELECT `key1`,`key2`,`val1`,`val2` FROM `myTable` WHERE (`key1`,`key2`) IN ((?,?),(?,?),(?,?))"

      when(mysqlClient.prepareAndSelect(anyString)).thenReturn(Future.value(new PreparedStatement(new PrepareOK(0, 0, 2, 0, Seq.empty[Field], Seq.empty[Field]))))
      when(mysqlClient.select(any[PreparedStatement])(anyFunction1[Row, ColumnMySqlValue])).thenReturn(Future.value(Seq.empty[ColumnMySqlValue]))

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

      val selectStatement = capture[PreparedStatement]
      verify(mysqlClient).select(selectStatement.capture)(anyFunction1[Row, ColumnMySqlValue])

      store.SELECT_SQL mustEqual selectSql

      val actualParameters = selectStatement.value.parameters.toSeq.map { case bytes: Array[Byte] => new String(bytes)}

      actualParameters must containTheSameElementsAs(Seq("1", "2", "100", "200", "1000", "2000"))
    }
  }
}
