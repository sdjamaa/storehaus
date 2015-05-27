package com.twitter.storehaus.mysql

import com.twitter.finagle.exp.mysql.{Result, PreparedStatement, Client}
import com.twitter.storehaus.{FutureOps, Store}
import com.twitter.util.{Time, Await, Future}

/**
 * Created by s.djamaa on 21/05/2015.
 */
class ColumnMySQLStore(client: Client, table: String, kCols: List[String], vCols: List[String])
  extends Store[ColumnMySqlValue, ColumnMySqlValue] {

  protected[mysql] val SELECT_SQL: String =
    "SELECT " + flatten(vCols) + " FROM " + g(table) + " WHERE " + kCols.map {
      g(_) + " =?"
    }.mkString(" AND ")

  protected[mysql] val MULTI_SELECT_SQL_PREFIX =
    "SELECT " + flatten(kCols) + ", " + flatten(vCols) +
      " FROM " + g(table) +
      " WHERE " + "(" + flatten(kCols) + ")" +
      " IN "

  protected[mysql] val UPDATE_SQL =
    "UPDATE " + g(table) +
    " SET " + vCols.map { g(_) + " =?" }.mkString(",") +
      " WHERE " + kCols.map {
      g(_) + " =?"
    }.mkString(" AND ")

  protected[mysql] val MULTI_UPDATE_SQL_PREFIX = "UPDATE " + g(table)
  protected[mysql] val MULTI_UPDATE_SQL_INFIX = " WHERE " + "(" + flatten(kCols) + ")" + " IN "

  protected[mysql] val INSERT_SQL =
    "INSERT INTO " + g(table) + " (" + flatten(kCols) + "," + flatten(vCols) + ")" +
    " VALUES " + Stream.continually("?").take(kCols.length + vCols.length).mkString("(", ",", ")")

  protected[mysql] val MULTI_INSERT_SQL_PREFIX =
    "INSERT INTO " + g(table) + "(" + flatten(kCols) + "," + flatten(vCols) + ") VALUES "

  private def flatten(cols: List[String]) = {
    cols.map {
      g(_)
    }.mkString(",")
  }

  protected[mysql] val DELETE_SQL = "DELETE FROM " + g(table) + " WHERE " + kCols.map {
    g(_) + " =?"
  }.mkString(" AND ")

  protected[mysql] val MULTI_DELETE_SQL_PREFIX = "DELETE FROM " + g(table) + " WHERE (" + flatten(kCols) + ") IN "

  protected val selectStmt = Await.result(client.prepare(SELECT_SQL))
  protected val deleteStmt = Await.result(client.prepare(DELETE_SQL))
  protected val updateStmt = Await.result(client.prepare(UPDATE_SQL))
  protected val insertStmt = Await.result(client.prepare(INSERT_SQL))

  protected val START_TXN_SQL = "START TRANSACTION"
  protected val COMMIT_TXN_SQL = "COMMIT"
  protected val ROLLBACK_TXN_SQL = "ROLLBACK"

  protected[mysql] def startTransaction: Future[Unit] = client.query(START_TXN_SQL).unit

  protected[mysql] def commitTransaction: Future[Unit] = client.query(COMMIT_TXN_SQL).unit

  protected[mysql] def rollbackTransaction: Future[Unit] = client.query(ROLLBACK_TXN_SQL).unit

  override def get(k: ColumnMySqlValue): Future[Option[ColumnMySqlValue]] = {
    selectStmt.parameters = ColumnMySqlStringInjection(k).map {
      _.getBytes
    }.toArray
    val mysqlResult: Future[Seq[ColumnMySqlValue]] = client.select(selectStmt) { row =>
      new ColumnMySqlValue(vCols.map { vCol => row(vCol) match {
        case Some(v) => MySqlValue(v)
      }
      }.toList)
    }
    // We only have one row in get
    mysqlResult.map { case result =>
      result.headOption
    }
  }

  /** Get a set of keys from the store.
    * Important: all keys in the input set are in the resulting map. If the store
    * fails to return a value for a given key, that should be represented by a
    * Future.exception.
    */
  override def multiGet[K1 <: ColumnMySqlValue](ks: Set[K1]): Map[K1, Future[Option[ColumnMySqlValue]]] = {
    if (ks.isEmpty) return Map()

    val placeholders = Stream.continually("?").take(ks.size).mkString("(", ",", ")")
    val params = Stream.continually(placeholders).take(kCols.size).mkString("(", ",", ")")
    val selectSql = MULTI_SELECT_SQL_PREFIX + params

    val mysqlResult: Future[(PreparedStatement, Seq[(ColumnMySqlValue, ColumnMySqlValue)])] =
      client.prepareAndSelect(selectSql, ks.flatMap(key => ColumnMySqlStringInjection(key).map(_.getBytes)).toSeq: _*) { row =>
        (new ColumnMySqlValue(kCols.map { kCol => row(kCol).map(MySqlValue(_)) match { case Some(v) => MySqlValue(v) } }),
         new ColumnMySqlValue(kCols.map { vCol => row(vCol).map(MySqlValue(_)) match { case Some(v) => MySqlValue(v) } }))
      }
    FutureOps.liftValues(ks,
    mysqlResult.map { case (ps, rows) =>
      client.closeStatement(ps)
      rows.toMap.filterKeys { _ != None }.map { case (optK, optV) => (optK, Some(optV)) }
    }, { (k: K1) => Future.None}
    )
  }

  override def put(kv: (ColumnMySqlValue, Option[ColumnMySqlValue])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => doSet(key, value).unit
      case (key, None) => doDelete(key).unit
    }
  }

  protected def doDelete(k: ColumnMySqlValue): Future[Result] = {
    deleteStmt.parameters = ColumnMySqlStringInjection(k).map {
      _.getBytes
    }.toArray
    client.execute(deleteStmt)
  }

  protected def doSet(k: ColumnMySqlValue, v: ColumnMySqlValue): Future[Result] = {
    get(k).flatMap { optionV =>
      optionV match {
        case Some(value) =>
          updateStmt.parameters = (ColumnMySqlStringInjection(k).map { _.getBytes } ++
            ColumnMySqlStringInjection(v).map { _.getBytes }).toArray
          client.execute(updateStmt)
        case None =>
          insertStmt.parameters = (ColumnMySqlStringInjection(k).map { _.getBytes } ++
            ColumnMySqlStringInjection(v).map { _.getBytes }).toArray

          client.execute(insertStmt)
      }
    }
  }

  protected def executeMultiInsert[K1 <: ColumnMySqlValue](kvs: Map[K1, ColumnMySqlValue]) = {
    val placeholders = Stream.continually("?").take(kCols.length + vCols.length).mkString("(", ",", ")")
    val params = Stream.continually(placeholders).take(kvs.size).mkString("(", ",", ")")

    val insertSql = MULTI_INSERT_SQL_PREFIX + params
    val insertParams = kvs.flatMap { kv =>
      ColumnMySqlStringInjection(kv._1).map { _.getBytes } ++ ColumnMySqlStringInjection(kv._2).map { _.getBytes }
    }.toSeq
    client.prepareAndExecute(insertSql, insertParams:_*).map { case (ps, r) =>
      // close prepared statement on server
      client.closeStatement(ps)
    }
  }

  protected def executeMultiUpdate[K1 <: ColumnMySqlValue](kvs: Map[K1, ColumnMySqlValue]) = {

    val whenClause = kCols.map { kCol => kCol + " = ? " }.mkString("WHEN ", " AND ", " THEN ? ") * kvs.size
    val setClause = vCols.map { vCol => " SET " + vCol + " = CASE " + whenClause + "END " }.mkString(",")

    val placeholders = Stream.continually("?").take(kCols.size).mkString("(", ",", ")")
    val params = Stream.continually(placeholders).take(kvs.size).mkString("(", ",", ")")

    val updateSql = MULTI_UPDATE_SQL_PREFIX + setClause + MULTI_UPDATE_SQL_INFIX + params

    val injectedKeys = kvs.keys.toSeq.flatMap { ColumnMySqlStringInjection(_).map { _.getBytes } }
    val injectedValues = kvs.values.toSeq.map { ColumnMySqlStringInjection(_).map { _.getBytes } }

    val multiUpdateCaseParams = for (i <- 0 until vCols.size;
                                 j <- 0 to kvs.size) yield
      injectedKeys(j) ++ injectedValues(j)(i)

    val updateInParams = kvs.flatMap { kv => ColumnMySqlStringInjection(kv._1).map { _.getBytes } }.toSeq

    client.prepareAndExecute(updateSql, (multiUpdateCaseParams ++ updateInParams):_*).map { case (ps, r) =>
      // close prepared statement on server
      client.closeStatement(ps)
    }
  }

  override def multiPut[K1 <: ColumnMySqlValue](kvs: Map[K1, Option[ColumnMySqlValue]]): Map[K1, Future[Unit]] = {
    // batched version of put. the batch is split into insert, update, and delete statements.
    // reduce your batch size if you are hitting mysql packet limit:
    // http://dev.mysql.com/doc/refman/5.1/en/packet-too-large.html
    val putResult = startTransaction.flatMap { t =>
      FutureOps.mapCollect(multiGet(kvs.keySet)).flatMap { result =>
        val existingKeys = result.filter { !_._2.isEmpty }.keySet
        val newKeys = result.filter { _._2.isEmpty }.keySet

        // handle inserts for new keys
        val insertF = newKeys.isEmpty match {
          case true => Future.Unit
          case false =>
            // do not include None values in insert query
            val insertKvs = newKeys.map { k => k -> kvs.getOrElse(k, None) }.filter { ! _._2.isEmpty }
              .toMap.mapValues { v => v.get }
            insertKvs.isEmpty match {
              case true => Future.Unit
              case false => executeMultiInsert(insertKvs)
            }
        }

        // handle update and/or delete for existing keys
        val existingKvs = existingKeys.map { k => k -> kvs.getOrElse(k, None) }

        // do not include None values in update query
        val updateKvs = existingKvs.filter { ! _._2.isEmpty }
          .toMap.mapValues { v => v.get }
        lazy val updateF = updateKvs.isEmpty match {
          case true => Future.Unit
          case false => executeMultiUpdate(updateKvs)
        }

        // deletes
        val deleteKeys = existingKvs.filter { _._2.isEmpty }.map { _._1 }
        lazy val deleteF = deleteKeys.isEmpty match {
          case true => Future.Unit
          case false =>
            val deleteSql = MULTI_DELETE_SQL_PREFIX + Stream.continually("?").take(deleteKeys.size).mkString("(", ",", ")")
            val deleteParams = deleteKeys.flatMap { k => ColumnMySqlStringInjection(k).map {_ .getBytes } }.toSeq
            client.prepareAndExecute(deleteSql, deleteParams:_*).map { case (ps, r) =>
              // close prepared statement on server
              client.closeStatement(ps)
            }
        }

        // sequence the three queries. the inner futures are lazy
        insertF.flatMap { f =>
          updateF.flatMap { f =>
            deleteF.flatMap { f => commitTransaction }
              .handle { case e: Exception => rollbackTransaction.flatMap { throw e } }
          }
        }
      }
    }
    kvs.mapValues { v => putResult.unit }
  }


  override def close(t: Time) = {
    // close prepared statements before closing the connection
    client.closeStatement(selectStmt)
    client.closeStatement(insertStmt)
    client.closeStatement(updateStmt)
    client.closeStatement(deleteStmt)
    client.close(t)
  }

  // enclose table or column names in backticks, in case they happen to be sql keywords
  protected def g(s: String) = "`" + s + "`"
}
