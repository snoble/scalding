package com.twitter.scalding.typed

import com.twitter.algebird._

/**
 * Extension for TypedPipe to add a cumulativeSum method
 */
object CumulativeSum {
  implicit class Extension[K, U, V](
    val pipe: TypedPipe[(K, (U, V))]
  )(
    implicit sg: Semigroup[V], ordU: Ordering[U], ordK: Ordering[K]
  ) extends AnyVal {
    /** Takes a sortable field and a monoid and returns the cumulative sum of that monoid **/
    def cumulativeSum: TypedPipe[(K, (U, V))] = {
      pipe.group
        .sortBy { case (u: U, _) => u }
        .scanLeft(Nil: List[(U, V)]) {
          case (acc, (u: U, v: V)) =>
            acc match {
              case List((previousU, previousSum)) => List((u, sg.plus(previousSum, v)))
              case _ => List((u, v))
            }
        }
        .flattenValues
        .toTypedPipe
    }
    /**
     * An optimization of cumulativeSum for cases when a particular key has many entries. Requires a sortable partitioning of U.
     * Accomplishes the optimization by not requiring all the entries for a single key to go through a single scan. Instead
     * requires the sums of the partitions for a single key to go through a single scan.*
     */
    def cumulativeSum[S](partition: U => S)(implicit ordS: Ordering[S]): TypedPipe[(K, (U, V))] = {

      val sumPerS = pipe
        .map { case (k, (u: U, v: V)) => (k, partition(u)) -> v }
        .sumByKey
        .map { case ((k, s), v) => (k, (s, v)) }
        .group
        .sortBy { case (s, v) => s }
        .scanLeft(None: Option[(Option[V], V, S)]) { case (acc, (s, v)) =>
          acc match {
            case Some((previousPreviousSum, previousSum, previousS)) => {
              Some((Some(previousSum), sg.plus(v, previousSum), s))
            }
            case _ => Some((None, v, s))
          }
        }
        .flatMap{
          case (k, maybeAcc) =>
            for (
              acc <- maybeAcc;
              previousSum <- acc._1
            ) yield { (k, acc._3) -> (None, previousSum) }
        }

        (
          pipe
            .map { case (k, (u: U, v: V)) => (k, partition(u)) -> (Some(u), v) } ++
            sumPerS
        )
          .group
          .sortBy { case (u, _) => u }
          .scanLeft(None: Option[(Option[U], V)]) { case (acc, (maybeU, v)) =>
            acc match {
              case Some((_, previousSum)) => Some((maybeU, sg.plus(v, previousSum)))
              case _ => Some((maybeU, v))
            }
          }
          .flatMap {case ((k, s), acc) =>
            for (uv <- acc; u <- uv._1) yield {
              (k, (u, uv._2))
            }
          }
    }
  }
}

