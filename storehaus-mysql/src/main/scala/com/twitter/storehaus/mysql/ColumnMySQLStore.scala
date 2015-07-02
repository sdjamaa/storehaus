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
    "SELECT " + flatten(kCols) + "," + flatten(vCols) +
      " FROM " + g(table) +
      " WHERE " + "(" + flatten(kCols) + ")" +
      " IN "

  protected[mysql] val UPDATE_SQL =
    "UPDATE " + g(table) +
      " SET " + vCols.map {
      g(_) + " =?"
    }.mkString(",") +
      " WHERE " + kCols.map {
      g(_) + " =?"
    }.mkString(" AND ")

  protected[mysql] val MULTI_UPDATE_SQL_PREFIX = "UPDATE " + g(table)
  protected[mysql] val MULTI_UPDATE_SQL_INFIX = " WHERE " + "(" + flatten(kCols) + ")" + " IN "

  protected[mysql] val INSERT_SQL =
    "INSERT INTO " + g(table) + " (" + flatten(kCols) + "," + flatten(vCols) + ")" +
      " VALUES " + Stream.continually("?").take(kCols.length + vCols.length).mkString("(", ",", ")")

  protected[mysql] val MULTI_INSERT_SQL_PREFIX =
    "INSERT INTO " + g(table) + " (" + flatten(kCols) + "," + flatten(vCols) + ") VALUES "

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

  protected[mysql] def startTransaction: Future[Unit] = {
    val res = client.query(START_TXN_SQL)
    res.unit
  }

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

    val placeholders = Stream.continually("?").take(kCols.size).mkString("(", ",", ")")
    val params = Stream.continually(placeholders).take(ks.size).mkString("(", ",", ")")
    val selectSql = MULTI_SELECT_SQL_PREFIX + params

    val selectParams = serializeMySqlValue(ks)

    val mysqlResult: Future[(PreparedStatement, Seq[(ColumnMySqlValue, ColumnMySqlValue)])] =
      client.prepareAndSelect(selectSql, selectParams: _*) { row => (
        new ColumnMySqlValue(kCols.map { kCol => row(kCol).map(MySqlValue(_)) match {
          case Some(v) => v
        }}),
        new ColumnMySqlValue(kCols.map { vCol => row(vCol).map(MySqlValue(_)) match {
          case Some(v) => v
        }}))
      }
    FutureOps.liftValues(ks,
    mysqlResult.map { case (ps, rows) =>
      client.closeStatement(ps)
      rows.toMap.filterKeys {
        _ != None
      }.map { case (optK, optV) => (optK, Some(optV))}
    }, { (k: K1) => Future.None}
    )
  }

  protected[mysql] def serializeMySqlValue[K1 <: ColumnMySqlValue](ks: Set[K1]): Array[Array[Byte]] = {
    ks.flatMap(key => ColumnMySqlStringInjection(key).map(_.getBytes)).toArray
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
          insertStmt.parameters = (ColumnMySqlStringInjection(k).map { _.getBytes} ++
            ColumnMySqlStringInjection(v).map { _.getBytes }).toArray

          client.execute(insertStmt)
      }
    }
  }

  protected[mysql] def executeMultiInsert[K1 <: ColumnMySqlValue](kvs: Map[K1, ColumnMySqlValue]) = {
    val placeholders = Stream.continually("?").take(kCols.length + vCols.length).mkString("(", ",", ")")
    val params = Stream.continually(placeholders).take(kvs.size).mkString(",")

    val insertSql = MULTI_INSERT_SQL_PREFIX + params
    val insertParams = serializeValues(kvs).toSeq
    client.prepareAndExecute(insertSql, insertParams: _*).map { case (ps, r) =>
      // close prepared statement on server
      client.closeStatement(ps)
    }
  }

  def serializeValues[K1 <: ColumnMySqlValue](kvs: Map[K1, ColumnMySqlValue]): Iterable[Array[Byte]] = {
    kvs.flatMap { kv =>
      ColumnMySqlStringInjection(kv._1).map { _.getBytes } ++ ColumnMySqlStringInjection(kv._2).map { _.getBytes }
    }
  }

  protected[mysql] def executeMultiUpdate[K1 <: ColumnMySqlValue](kvs: Map[K1, ColumnMySqlValue]) = {

    val whenClause = kCols.map { kCol => g(kCol) + " =?"}.mkString("WHEN ", " AND ", " THEN ? ") * kvs.size
    val setClause = vCols.map { vCol => " SET " + g(vCol) + " = CASE " + whenClause + "END"}.mkString(",")

    val placeholders = Stream.continually("?").take(kCols.size).mkString("(", ",", ")")
    val params = Stream.continually(placeholders).take(kvs.size).mkString("(", ",", ")")

    val updateSql = MULTI_UPDATE_SQL_PREFIX + setClause + MULTI_UPDATE_SQL_INFIX + params

    val updateParams = serializeKeyAndValueForUpdate(kvs)

    client.prepareAndExecute(updateSql, updateParams: _*).map { case (ps, r) =>
      // close prepared statement on server
      client.closeStatement(ps)
    }
  }

  def serializeKeyAndValueForUpdate[K1 <: ColumnMySqlValue](kvs: Map[K1, ColumnMySqlValue]) = {
    val injectedKeys = kvs.keys.map {
      ColumnMySqlStringInjection(_).map { _.getBytes }
    }.toList

    val injectedValues = kvs.values.toSeq.map {
      ColumnMySqlStringInjection(_).map { _.getBytes }
    }.transpose.toList

    val multiUpdateCaseParams = injectedValues.flatMap { v =>
      for (i <- 0 until kvs.size) yield (injectedKeys(i) :+ v(i))
    }.flatten.toList

    val updateInParams = kvs.flatMap { kv => ColumnMySqlStringInjection(kv._1).map { _.getBytes } }.toSeq

    multiUpdateCaseParams ++ updateInParams
  }

  override def multiPut[K1 <: ColumnMySqlValue](kvs: Map[K1, Option[ColumnMySqlValue]]): Map[K1, Future[Unit]] = {
    // batched version of put. the batch is split into insert, update, and delete statements.
    // reduce your batch size if you are hitting mysql packet limit:
    // http://dev.mysql.com/doc/refman/5.1/en/packet-too-large.html
    val putResult = startTransaction.flatMap { t =>
      FutureOps.mapCollect(multiGet(kvs.keySet)).flatMap { result =>
        val existingKeys = result.filter {
          !_._2.isEmpty
        }.keySet
        val newKeys = result.filter {
          _._2.isEmpty
        }.keySet

        // handle inserts for new keys
        val insertF = newKeys.isEmpty match {
          case true => Future.Unit
          case false =>
            // do not include None values in insert query
            val insertKvs = newKeys.map { k => k -> kvs.getOrElse(k, None)}.filter {
              !_._2.isEmpty
            }
              .toMap.mapValues { v => v.get}
            insertKvs.isEmpty match {
              case true => Future.Unit
              case false => executeMultiInsert(insertKvs)
            }
        }

        // handle update and/or delete for existing keys
        val existingKvs = existingKeys.map { k => k -> kvs.getOrElse(k, None)}

        // do not include None values in update query
        val updateKvs = existingKvs.filter {
          !_._2.isEmpty
        }
          .toMap.mapValues { v => v.get}
        lazy val updateF = updateKvs.isEmpty match {
          case true => Future.Unit
          case false => executeMultiUpdate(updateKvs)
        }

        // deletes
        val deleteKeys = existingKvs.filter {
          _._2.isEmpty
        }.map {
          _._1
        }
        lazy val deleteF = deleteKeys.isEmpty match {
          case true => Future.Unit
          case false =>
            val deleteSql = {
              val placeholders = Stream.continually("?").take(kCols.size).mkString("(", ",", ")")
              val paramHolders = Stream.continually(placeholders).take(deleteKeys.size).mkString("(", ",", ")")
              MULTI_DELETE_SQL_PREFIX + paramHolders
            }
            val deleteParams = serializeKeys(deleteKeys).toSeq
            client.prepareAndExecute(deleteSql, deleteParams: _*).map { case (ps, r) =>
              // close prepared statement on server
              client.closeStatement(ps)
            }
        }

        // sequence the three queries. the inner futures are lazy
        insertF.flatMap { f =>
          updateF.flatMap { f =>
            deleteF.flatMap { f => commitTransaction}
              .handle { case e: Exception => rollbackTransaction.flatMap {
              throw e
            }
            }
          }
        }
      }
    }
    kvs.mapValues { v => putResult.unit}
  }


  def serializeKeys[K1 <: ColumnMySqlValue](deleteKeys: Set[K1]): Set[Array[Byte]] = {
    deleteKeys.flatMap { k => ColumnMySqlStringInjection(k).map {
      _.getBytes
    }
    }
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

object ColumnMySQLStore {
  def apply(client: Client, table: String, kCols: List[String], vCols: List[String]) = {
    new ColumnMySQLStore(client, table, kCols, vCols)
  }
}