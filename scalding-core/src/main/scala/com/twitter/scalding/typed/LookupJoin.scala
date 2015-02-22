/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.scalding.typed

import java.io.Serializable

import com.twitter.scalding.typed.CumulativeSum._
import com.twitter.algebird.Semigroup
import com.twitter.algebird.Last

/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

/**
 * lookupJoin simulates the behavior of a realtime system attempting
 * to leftJoin (K, V) pairs against some other value type (JoinedV)
 * by performing realtime lookups on a key-value Store.
 *
 * An example would join (K, V) pairs of (URL, Username) against a
 * service of (URL, ImpressionCount). The result of this join would
 * be a pipe of (ShortenedURL, (Username,
 * Option[ImpressionCount])).
 *
 * To simulate this behavior, lookupJoin accepts pipes of key-value
 * pairs with an explicit time value T attached. T must have some
 * sensible ordering. The semantics are, if one were to hit the
 * right pipe's simulated realtime service at any time between
 * T(tuple) T(tuple + 1), one would receive Some((K,
 * JoinedV)(tuple)).
 *
 * The entries in the left pipe's tuples have the following
 * meaning:
 *
 * T: The  time at which the (K, W) lookup occurred.
 * K: the join key.
 * W: the current value for the join key.
 *
 * The right pipe's entries have the following meaning:
 *
 * T: The time at which the "service" was fed an update
 * K: the join K.
 * V: value of the key at time T
 *
 * Before the time T in the right pipe's very first entry, the
 * simulated "service" will return None. After this time T, the
 * right side will return None only if the key is absent,
 * else, the service will return Some(joinedV).
 */

object LookupJoin extends Serializable {

  /**
   * This is the "infinite history" join and always joins regardless of how
   * much time is between the left and the right
   */

  def apply[T: Ordering, K: Ordering, V, JoinedV](
    left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None): TypedPipe[(T, (K, (V, Option[JoinedV])))] =

    withWindow(left, right, reducers)((_, _) => true)

  /**
   * In this case, the right pipe is fed through a scanLeft doing a Semigroup.plus
   * before joined to the left
   */
  def rightSumming[T: Ordering, K: Ordering, V, JoinedV: Semigroup](left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None): TypedPipe[(T, (K, (V, Option[JoinedV])))] =
    withWindowRightSumming(left, right, reducers)((_, _) => true)

  /**
   * This ensures that gate(Tleft, Tright) == true, else the None is emitted
   * as the joined value.
   * Useful for bounding the time of the join to a recent window
   */
  def withWindow[T: Ordering, K: Ordering, V, JoinedV](left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None)(gate: (T, T) => Boolean): TypedPipe[(T, (K, (V, Option[JoinedV])))] = {

    implicit val keepNew: Semigroup[JoinedV] = Semigroup.from { (older, newer) => newer }
    withWindowRightSumming(left, right, reducers)(gate)
  }

  /**
   * This ensures that gate(Tleft, Tright) == true, else the None is emitted
   * as the joined value, and sums are only done as long as they they come
   * within the gate interval as well
   */
  def withWindowRightSumming[T: Ordering, K: Ordering, V, JoinedV: Semigroup](left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None)(gate: (T, T) => Boolean): TypedPipe[(T, (K, (V, Option[JoinedV])))] = {
    withWindowRightSummingAndOptionalPartition(
      left, right, reducers)(gate, { _.cumulativeSum(reducers.getOrElse(-1)) })
  }

  def withWindowRightSummingAndPartition[T: Ordering, S: Ordering, K: Ordering, V, JoinedV: Semigroup](left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None)(
      gate: (T, T) => Boolean, partition: T => S): TypedPipe[(T, (K, (V, Option[JoinedV])))] = {
    withWindowRightSummingAndOptionalPartition(
      left, right, reducers)(gate, { _.cumulativeSum(reducers.getOrElse(-1), partition) })
  }

  private def withWindowRightSummingAndOptionalPartition[T: Ordering, K: Ordering, V, JoinedV: Semigroup](left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None)(
      gate: (T, T) => Boolean,
      cumulate: (TypedPipe[(K, (T, (Last[Option[V]], Option[(Last[T], JoinedV)])))]) => TypedPipe[(K, (T, (Last[Option[V]], Option[(Last[T], JoinedV)])))]): TypedPipe[(T, (K, (V, Option[JoinedV])))] = {
    implicit val gatedRightSum: Semigroup[(T, JoinedV)] = Semigroup.from {
      case ((leftT, leftV), (rightT, rightV)) => if (gate(leftT, rightT)) {
        (rightT, Semigroup.plus(leftV, rightV))
      } else {
        (rightT, rightV)
      }
    }

    val concatinated: TypedPipe[(K, (T, (Last[Option[V]], Option[(Last[T], JoinedV)])))] =
      left.map { case (t, (k, v)) => (k, (t, (Last(Option(v)), None: Option[(Last[T], JoinedV)]))) }
        .++(right.map {
          case (t, (k, joinedV)) =>
            (k, (t, (Last(None: Option[V]), Option((Last(t), joinedV)))))
        })

    val cumulativeSummed = cumulate(concatinated)
    cumulativeSummed
      .collect {
        case (k, (t, (Last(Some(v)), joinedTV))) =>
          val joinedV = joinedTV.collect { case (joinedT, joinedV) if gate(joinedT.get, t) => joinedV }
          (t, (k, (v, joinedV)))
      }
  }
}
