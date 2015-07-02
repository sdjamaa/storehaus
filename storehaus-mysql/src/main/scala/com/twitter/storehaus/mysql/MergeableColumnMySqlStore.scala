package com.twitter.storehaus.mysql

import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.bijection.Injection
import com.twitter.finagle.exp.Mysql
import com.twitter.finagle.exp.mysql.Client
import com.twitter.storehaus.{Store, FutureOps, ConvertedStore}
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.Future

/**
 * Created by s.djamaa on 01/06/2015.
 */

object MergeableColumnMySqlStore {

  def typed[K, V](url: String, user: String, password: String, dbName: String, tableName: String, kCol: List[String], vCol: List[String])
                 (implicit kInj: Injection[K, ColumnMySqlValue], vInj: Injection[V, ColumnMySqlValue]): Store[K, V] = {

    val client = Client(Mysql
      .withCredentials(user, password)
      .withDatabase(dbName)
      .newClient(url))

    ColumnMySQLStore(client, tableName, kCol, vCol).convert[K, V](kInj)
  }

  def mergeable[K, V](url: String, user: String, password: String, dbName: String, tableName: String, kCol: List[String], vCol: List[String])
                     (implicit kInj: Injection[K, ColumnMySqlValue], vInj: Injection[V, ColumnMySqlValue], vMonoid: Monoid[V]): MergeableStore[K, V] =
    MergeableStore.fromStore(
      typed(url, user, password, dbName, tableName, kCol, vCol)
    )


}

class MergeableColumnMySqlStore[V](underlying: ColumnMySQLStore)(implicit inj: Injection[V, ColumnMySqlValue],
                                                                 override val semigroup: Semigroup[V])
  extends ConvertedStore[ColumnMySqlValue, ColumnMySqlValue, ColumnMySqlValue, V](underlying)(identity)
  with MergeableStore[ColumnMySqlValue, V] {

  // Merges multiple keys inside a transaction.
  // 1. existing keys are fetched using multiGet (SELECT query)
  // 2. new keys are added using INSERT query
  // 3. existing keys are merged using UPDATE query
  // NOTE: merge on a single key also in turn calls this
  override def multiMerge[K1 <: ColumnMySqlValue](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = {
    val mergeResult: Future[Map[K1, Option[V]]] = underlying.startTransaction.flatMap { u: Unit =>
      FutureOps.mapCollect(multiGet(kvs.keySet)).flatMap { result: Map[K1, Option[V]] =>
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
            val insertKvs = newKeys.map { k => k -> kvs.get(k).get}
            insertKvs.isEmpty match {
              case true => Future.Unit
              case false => underlying.executeMultiInsert(insertKvs.toMap.mapValues { v => inj(v)})
            }
        }

        // handle update/merge for existing keys
        // lazy val realized inside of insertF.flatMap
        lazy val updateF = existingKeys.isEmpty match {
          case true => Future.Unit
          case false =>
            val existingKvs = existingKeys.map { k => k -> kvs.get(k).get}
            underlying.executeMultiUpdate(existingKvs.map { kv =>
              val resV = semigroup.plus(result.get(kv._1).get.get, kv._2)
              kv._1 -> inj(resV)
            }.toMap)
        }

        // insert, update and commit or rollback accordingly
        insertF.flatMap { f =>
          updateF.flatMap { f =>
            underlying.commitTransaction.map { f =>
              // return values before the merge
              result
            }
          }
            .onFailure { case e: Exception =>
            underlying.rollbackTransaction.map { f =>
              // map values to exception
              result.mapValues { v => e}
            }
          }
        }
      }
    }
    FutureOps.liftValues(kvs.keySet, mergeResult)
  }
}