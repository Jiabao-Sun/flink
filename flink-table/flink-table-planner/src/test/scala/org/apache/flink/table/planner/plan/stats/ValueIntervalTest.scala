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
package org.apache.flink.table.planner.plan.stats

import org.apache.flink.table.planner.plan.stats.ValueInterval._
import org.apache.flink.table.planner.plan.utils.ColumnIntervalUtil.toBigDecimalInterval
import org.assertj.core.api.Assertions.{assertThat, assertThatExceptionOfType}
import org.junit.jupiter.api.Test

class ValueIntervalTest {

  @Test
  def testUnion(): Unit = {
    // empty union empty = empty
    assertThat(union(empty, empty)).isEqualTo(empty)
    // infinity union infinity = infinity
    assertThat(union(infinite, infinite)).isEqualTo(infinite)
    // empty union [1, 3] = [1, 3]
    val interval1 = ValueInterval(1, 3)
    assertThat(union(empty, interval1)).isEqualTo(interval1)
    assertThat(union(interval1, empty)).isEqualTo(interval1)
    // infinity union [1,3] = infinity
    assertThat(union(infinite, interval1)).isEqualTo(infinite)
    assertThat(union(interval1, infinite)).isEqualTo(infinite)
    // [1, 3] union (-1, 2) = (-1, 3]
    assertThat(
      union(interval1, ValueInterval(-1, 2, includeLower = false, includeUpper = true)))
      .isEqualTo(ValueInterval(-1, 3, includeLower = false, includeUpper = true))
    // [1, 3] union [-1, 4] = [-1, 4]
    assertThat(union(interval1, ValueInterval(-1, 4))).isEqualTo(ValueInterval(-1, 4))
    // [1, 3] union [-3, -2) = [-3, 3]
    assertThat(
      union(interval1, ValueInterval(-3, -2, includeLower = true, includeUpper = false)))
      .isEqualTo(ValueInterval(-3, 3))
    // [1, 3] union (0, 3) = (0, 3]
    assertThat(
      union(interval1, ValueInterval(0, 3, includeLower = false, includeUpper = false)))
      .isEqualTo(ValueInterval(0, 3, includeLower = false, includeUpper = true))
    // [1, 3] union (0, 4] = (0, 4]
    assertThat(
      union(interval1, ValueInterval(0, 4, includeLower = false, includeUpper = true)))
      .isEqualTo(ValueInterval(0, 4, includeLower = false, includeUpper = true))
    // [1, 3] union [4, 7] = [1,7]
    assertThat(union(interval1, ValueInterval(4, 7))).isEqualTo(ValueInterval(1, 7))
    // [1, 3] union (-Inf, -2) = (-Inf, 3]
    assertThat(union(interval1, ValueInterval(null, -2, includeUpper = false)))
      .isEqualTo(ValueInterval(null, 3))
    assertThat(union(ValueInterval(null, -2, includeUpper = false), interval1))
      .isEqualTo(ValueInterval(null, 3))
    // [1, 3] union (-Inf, 3) = (-Inf, 3]
    assertThat(union(interval1, ValueInterval(null, 3, includeUpper = false)))
      .isEqualTo(ValueInterval(null, 3))
    assertThat(union(ValueInterval(null, 3, includeUpper = false), interval1))
      .isEqualTo(ValueInterval(null, 3))
    // [1, 3] union (-Inf, 4] = (-Inf, 4]
    assertThat(union(interval1, ValueInterval(null, 4)))
      .isEqualTo(ValueInterval(null, 4))
    assertThat(union(ValueInterval(null, 4), interval1))
      .isEqualTo(ValueInterval(null, 4))
    // [1, 3] union [-1, Inf) = [-1, Inf)
    assertThat(union(interval1, ValueInterval(-1, null)))
      .isEqualTo(ValueInterval(-1, null))
    assertThat(union(ValueInterval(-1, null), interval1))
      .isEqualTo(ValueInterval(-1, null))
    // [1, 3] union [0, Inf) = [0, Inf)
    assertThat(union(interval1, ValueInterval(0, null)))
      .isEqualTo(ValueInterval(0, null))
    assertThat(union(ValueInterval(0, null), interval1))
      .isEqualTo(ValueInterval(0, null))
    // [1, 3] union [4, Inf) = [1, Inf)
    assertThat(union(interval1, ValueInterval(4, null)))
      .isEqualTo(ValueInterval(1, null))
    assertThat(union(ValueInterval(4, null), interval1))
      .isEqualTo(ValueInterval(1, null))
    val interval2 = ValueInterval(null, 2)
    // (-Inf, 2] union [3, Inf) = infinity
    assertThat(union(interval2, ValueInterval(3, null))).isEqualTo(infinite)
    assertThat(union(ValueInterval(3, null), interval2)).isEqualTo(infinite)
    // (-Inf, 2] union [-1, Inf) = infinity
    assertThat(union(interval2, ValueInterval(-1, null))).isEqualTo(infinite)
    assertThat(union(ValueInterval(-1, null), interval2)).isEqualTo(infinite)
    // (-Inf, 2] union (-Inf, 1) = (-Inf, 2]
    assertThat(
      union(interval2, ValueInterval(null, 1, includeUpper = false)))
      .isEqualTo(interval2)
    // (-Inf, 2] union (-Inf, 3) = (-Inf, 3)
    assertThat(
      union(interval2, ValueInterval(null, 3, includeUpper = false)))
      .isEqualTo(ValueInterval(null, 3, includeUpper = false))
    val interval3 = ValueInterval(3, null)
    // [3, Inf) union (4, Inf) = [3, Inf)
    assertThat(
      union(interval3, ValueInterval(4, null, includeLower = false)))
      .isEqualTo(interval3)
    // [3, Inf) union (-1, Inf) = (-1, Inf)
    assertThat(
      union(interval3, ValueInterval(null, null, includeLower = false)))
      .isEqualTo(ValueInterval(-1, null, includeLower = false))
  }

  @Test
  def testUnionUncompatibility(): Unit = {
    assertThat(toBigDecimalInterval(
      union(ValueInterval(1L, 2L), ValueInterval(1.2d, 2.0d))
    )).isEqualTo(toBigDecimalInterval(ValueInterval(1, 2)))
  }

  @Test
  def testIsIntersected(): Unit = {
    // empty not intersect empty
    assertThat(isIntersected(empty, empty)).isFalse
    // infinity intersect infinity
    assertThat(isIntersected(infinite, infinite)).isTrue
    val interval1 = ValueInterval(1, 3)
    // empty not intersect [1, 3]
    assertThat(isIntersected(empty, interval1)).isFalse
    assertThat(isIntersected(interval1, empty)).isFalse
    // infinity intersect [1,3]
    assertThat(isIntersected(infinite, interval1)).isTrue
    assertThat(isIntersected(interval1, infinite)).isTrue
    // [1, 3] intersect (-1, 2)
    assertThat(
      isIntersected(interval1, ValueInterval(-1, 2, includeLower = false, includeUpper = false))).isTrue
    // [1, 3] intersect [-1, 4]
    assertThat(isIntersected(interval1, ValueInterval(-1, 4))).isTrue
    // [1, 3] not intersect [-3, -2)
    assertThat(
      isIntersected(interval1, ValueInterval(-3, -2, includeLower = true, includeUpper = false))).isFalse
    // [1, 3] intersect (0, 3)
    assertThat(
      isIntersected(interval1, ValueInterval(0, 3, includeLower = false, includeUpper = false))).isTrue
    // [1, 3] intersect (0, 4]
    assertThat(
      isIntersected(interval1, ValueInterval(0, 4, includeLower = false, includeUpper = true))).isTrue
    // [1, 3] not intersect [4, 7]
    assertThat(isIntersected(interval1, ValueInterval(4, 7))).isFalse
    // [1, 3) not intersect [3, 3]
    assertThat(
      isIntersected(
        ValueInterval(1, 3, includeLower = true, includeUpper = false),
        ValueInterval(3, 3))).isFalse
    // [1, 3] not intersect (-Inf, -2]
    assertThat(isIntersected(interval1, ValueInterval(null, -2))).isFalse
    assertThat(isIntersected(ValueInterval(null, -2), interval1)).isFalse
    // [1, 3] intersect (-Inf, 3)
    assertThat(isIntersected(interval1, ValueInterval(null, 3, includeUpper = false))).isTrue
    assertThat(isIntersected(ValueInterval(null, 3, includeUpper = false), interval1)).isTrue
    // [1, 3] intersect (-Inf, 4]
    assertThat(isIntersected(interval1, ValueInterval(null, 4))).isTrue
    assertThat(isIntersected(ValueInterval(null, 4), interval1)).isTrue
    // [1, 3] intersect [-1, Inf)
    assertThat(isIntersected(interval1, ValueInterval(-1, null))).isTrue
    assertThat(isIntersected(ValueInterval(-1, null), interval1)).isTrue
    // [1, 3] intersect [0, Inf)
    assertThat(isIntersected(interval1, ValueInterval(0, null))).isTrue
    assertThat(isIntersected(ValueInterval(0, null), interval1)).isTrue
    // [1, 3] not intersect [4, Inf)
    assertThat(isIntersected(interval1, ValueInterval(4, null))).isFalse
    assertThat(isIntersected(ValueInterval(4, null), interval1)).isFalse
    // [1, 3] not intersect (3, Inf)
    assertThat(isIntersected(interval1, ValueInterval(3, null, includeLower = false))).isFalse
    assertThat(isIntersected(ValueInterval(3, null, includeLower = false), interval1)).isFalse
    val interval2 = ValueInterval(null, 2)
    // (-Inf, 2] not intersect [3, Inf)
    assertThat(isIntersected(interval2, ValueInterval(3, null))).isFalse
    assertThat(isIntersected(ValueInterval(3, null), interval2)).isFalse
    // (-Inf, 2] intersect [-1, Inf)
    assertThat(isIntersected(interval2, ValueInterval(-1, null))).isTrue
    assertThat(isIntersected(ValueInterval(-1, null), interval2)).isTrue
    // (-Inf, 2] not intersect (2, Inf)
    assertThat(isIntersected(interval2, ValueInterval(2, null, includeLower = false))).isFalse
    assertThat(isIntersected(ValueInterval(2, null, includeLower = false), interval2)).isFalse
    // (-Inf, 2] intersect [2, Inf)
    assertThat(isIntersected(interval2, ValueInterval(2, null))).isTrue
    assertThat(isIntersected(ValueInterval(2, null), interval2)).isTrue
    // (-Inf, 2] intersect (-Inf, 1)
    assertThat(isIntersected(interval2, ValueInterval(null, 1, includeUpper = false))).isTrue
    // (-Inf, 2] intersect (-Inf, 3)
    assertThat(isIntersected(interval2, ValueInterval(null, 3, includeUpper = false))).isTrue
    // [3, Inf) intersect [4, Inf)
    assertThat(isIntersected(ValueInterval(3, null), ValueInterval(4, null))).isTrue
    // [3, Inf) intersect (-1, Inf)
    assertThat(isIntersected(ValueInterval(3, null), ValueInterval(-1, null, includeLower = false))).isTrue
    // [1, 5] intersect [2.0, 3.0]
    assertThat(isIntersected(ValueInterval(1, 5), ValueInterval(2.0d, 3.0d))).isTrue
  }

  @Test
  def testIntersect(): Unit = {
    // empty intersect empty = empty
    assertThat(intersect(empty, empty)).isEqualTo(empty)
    // infinity intersect infinity = infinity
    assertThat(intersect(infinite, infinite)).isEqualTo(infinite)
    val interval1 = ValueInterval(1, 3)
    // empty intersect [1, 3] = empty
    assertThat(intersect(empty, interval1)).isEqualTo(empty)
    assertThat(intersect(interval1, empty)).isEqualTo(empty)
    // infinity intersect [1,3] = [1,3]
    assertThat(intersect(infinite, interval1)).isEqualTo(interval1)
    assertThat(intersect(interval1, infinite)).isEqualTo(interval1)
    // [1, 3] intersect (-1, 2) = [1, 2)
    assertThat(
      intersect(interval1, ValueInterval(-1, 2, includeLower = false, includeUpper = false)))
      .isEqualTo(ValueInterval(1, 2, includeLower = true, includeUpper = false))
    // [1, 3] intersect [-1, 4] = [1, 3]
    assertThat(intersect(interval1, ValueInterval(-1, 4))).isEqualTo(interval1)
    // [1, 3] intersect [-3, -2) = empty
    assertThat(
      intersect(interval1, ValueInterval(-3, -2, includeLower = true, includeUpper = false)))
      .isEqualTo(empty)
    // [1, 3] intersect (0, 3) = [1, 3)
    assertThat(
      intersect(interval1, ValueInterval(0, 3, includeLower = false, includeUpper = false)))
      .isEqualTo(ValueInterval(1, 3, includeLower = true, includeUpper = false))
    // [1, 3] intersect (0, 4] = [1, 3]
    assertThat(intersect(interval1, ValueInterval(0, 4, includeLower = false, includeUpper = true)))
      .isEqualTo(interval1)
    // [1, 3] intersect [4, 7] = empty
    assertThat(intersect(interval1, ValueInterval(4, 7))).isEqualTo(empty)
    // [1, 3) intersect [3, 3] = empty
    assertThat(intersect(
      ValueInterval(1, 3, includeLower = true, includeUpper = false),
      ValueInterval(3, 3)))
      .isEqualTo(empty)
    // [1, 3] intersect (-Inf, -2] = empty
    assertThat(intersect(interval1, ValueInterval(null, -2))).isEqualTo(empty)
    assertThat(intersect(ValueInterval(null, -2), interval1)).isEqualTo(empty)
    // [1, 3] intersect (-Inf, 3) = [1, 3)
    assertThat(intersect(interval1, ValueInterval(null, 3, includeUpper = false)))
      .isEqualTo(ValueInterval(1, 3, includeLower = true, includeUpper = false))
    assertThat(intersect(ValueInterval(null, 3, includeUpper = false), interval1))
      .isEqualTo(ValueInterval(1, 3, includeLower = true, includeUpper = false))
    // [1, 3] intersect (-Inf, 4] = [1, 3]
    assertThat(intersect(interval1, ValueInterval(null, 4))).isEqualTo(interval1)
    assertThat(intersect(ValueInterval(null, 4), interval1)).isEqualTo(interval1)
    // [1, 3] intersect [-1, Inf) = [1, 3]
    assertThat(intersect(interval1, ValueInterval(-1, null))).isEqualTo(interval1)
    assertThat(intersect(ValueInterval(-1, null), interval1)).isEqualTo(interval1)
    // [1, 3] intersect [0, Inf) = [1, 3]
    assertThat(intersect(interval1, ValueInterval(0, null))).isEqualTo(interval1)
    assertThat(intersect(ValueInterval(0, null), interval1)).isEqualTo(interval1)
    // [1, 3] intersect [4, Inf) = empty
    assertThat(intersect(interval1, ValueInterval(4, null))).isEqualTo(empty)
    assertThat(intersect(ValueInterval(4, null), interval1)).isEqualTo(empty)
    // [1, 3] intersect (3, Inf) = empty
    assertThat(intersect(interval1, ValueInterval(3, null, includeLower = false))).isEqualTo(empty)
    assertThat(intersect(ValueInterval(3, null, includeLower = false), interval1)).isEqualTo(empty)
    val interval2 = ValueInterval(null, 2)
    // (-Inf, 2] intersect [3, Inf) = empty
    assertThat(intersect(interval2, ValueInterval(3, null))).isEqualTo(empty)
    assertThat(intersect(ValueInterval(3, null), interval2)).isEqualTo(empty)
    // (-Inf, 2] intersect [-1, Inf) = [-1, 2]
    assertThat(intersect(interval2, ValueInterval(-1, null))).isEqualTo(ValueInterval(-1, 2))
    assertThat(intersect(ValueInterval(-1, null), interval2)).isEqualTo(ValueInterval(-1, 2))
    // (-Inf, 2] intersect (2, Inf) = empty
    assertThat(intersect(interval2, ValueInterval(2, null, includeLower = false))).isEqualTo(empty)
    assertThat(intersect(ValueInterval(2, null, includeLower = false), interval2)).isEqualTo(empty)
    // (-Inf, 2] intersect [2, Inf) = [2, 2]
    assertThat(intersect(interval2, ValueInterval(2, null))).isEqualTo(ValueInterval(2, 2))
    assertThat(intersect(ValueInterval(2, null), interval2)).isEqualTo(ValueInterval(2, 2))
    // (-Inf, 2] intersect (-Inf, 1) = (-Inf, 1)
    assertThat(intersect(interval2, ValueInterval(null, 1, includeUpper = false))).isEqualTo(ValueInterval(null, 1, includeUpper = false))
    // (-Inf, 2] intersect (-Inf, 3) = (-Inf, 2]
    assertThat(intersect(interval2, ValueInterval(null, 3, includeUpper = false))).isEqualTo(interval2)
    // [3, Inf) intersect [4, Inf) = [4, Inf)
    assertThat(intersect(ValueInterval(3, null), ValueInterval(4, null))).isEqualTo(ValueInterval(4, null))
    // [3, Inf) intersect (-1, Inf) = [3, Inf)
    assertThat(intersect(ValueInterval(3, null), ValueInterval(-1, null, includeLower = false)))
      .isEqualTo(ValueInterval(3, null))
  }

  @Test
  def testContains(): Unit = {
    // EmptyValueInterval
    assertThat(contains(empty, null))
    assertThat(contains(empty, 0))
    assertThat(contains(empty, 1.0))
    assertThat(contains(empty, "abc"))

    // InfiniteValueInterval
    assertThat(contains(infinite, null))
    assertThat(contains(infinite, 0)).isTrue
    assertThat(contains(infinite, 1.0)).isTrue
    assertThat(contains(infinite, "abc")).isTrue

    // LeftSemiInfiniteValueInterval
    // int type
    val intInterval1 = ValueInterval(null, 100, includeLower = false, includeUpper = true)
    assertThat(contains(intInterval1, null))
    assertThat(contains(intInterval1, 99))
    assertThat(contains(intInterval1, 100)).isTrue
    assertThat(contains(intInterval1, 101)).isTrue
    val intInterval2 = ValueInterval(null, 100, includeLower = false, includeUpper = false)
    assertThat(contains(intInterval2, null))
    assertThat(contains(intInterval2, 99)).isTrue
    assertThat(contains(intInterval2, 100))
    assertThat(contains(intInterval2, 101))
    // string type
    val strInterval1 = ValueInterval(null, "m", includeLower = false, includeUpper = true)
    assertThat(contains(strInterval1, null))
    assertThat(contains(strInterval1, "l")).isTrue
    assertThat(contains(strInterval1, "m")).isTrue
    assertThat(contains(strInterval1, "n"))
    val strInterval2 = ValueInterval(null, "m", includeLower = false, includeUpper = false)
    assertThat(contains(strInterval2, null))
    assertThat(contains(strInterval2, "l")).isTrue
    assertThat(contains(strInterval2, "m"))
    assertThat(contains(strInterval2, "n"))

    // RightSemiInfiniteValueInterval
    // int type
    val intInterval3 = ValueInterval(100, null, includeLower = true, includeUpper = false)
    assertThat(contains(intInterval3, null))
    assertThat(contains(intInterval3, 99))
    assertThat(contains(intInterval3, 100)).isTrue
    assertThat(contains(intInterval3, 101)).isTrue
    val intInterval4 = ValueInterval(100, null, includeLower = false, includeUpper = false)
    assertThat(contains(intInterval4, null))
    assertThat(contains(intInterval4, 99))
    assertThat(contains(intInterval4, 100))
    assertThat(contains(intInterval4, 101)).isTrue
    // string type
    val strInterval3 = ValueInterval("m", null, includeLower = true, includeUpper = false)
    assertThat(contains(strInterval3, null))
    assertThat(contains(strInterval3, "l"))
    assertThat(contains(strInterval3, "m")).isTrue
    assertThat(contains(strInterval3, "n")).isTrue
    val strInterval4 = ValueInterval("m", null, includeLower = false, includeUpper = false)
    assertThat(contains(strInterval4, null))
    assertThat(contains(strInterval4, "l"))
    assertThat(contains(strInterval4, "m"))
    assertThat(contains(strInterval4, "n")).isTrue

    // FiniteValueInterval
    // int type
    val intInterval5 = ValueInterval(100, 200, includeLower = true, includeUpper = true)
    assertThat(contains(intInterval5, null))
    assertThat(contains(intInterval5, 99))
    assertThat(contains(intInterval5, 100)).isTrue
    assertThat(contains(intInterval5, 101)).isTrue
    assertThat(contains(intInterval5, 199)).isTrue
    assertThat(contains(intInterval5, 200)).isTrue
    assertThat(contains(intInterval5, 201))
    val intInterval6 = ValueInterval(100, 200, includeLower = false, includeUpper = true)
    assertThat(contains(intInterval6, null))
    assertThat(contains(intInterval6, 99))
    assertThat(contains(intInterval6, 100))
    assertThat(contains(intInterval6, 101)).isTrue
    assertThat(contains(intInterval6, 199)).isTrue
    assertThat(contains(intInterval6, 200)).isTrue
    assertThat(contains(intInterval6, 201))
    val intInterval7 = ValueInterval(100, 200, includeLower = true, includeUpper = false)
    assertThat(contains(intInterval7, null))
    assertThat(contains(intInterval7, 99))
    assertThat(contains(intInterval7, 100)).isTrue
    assertThat(contains(intInterval7, 101)).isTrue
    assertThat(contains(intInterval7, 199)).isTrue
    assertThat(contains(intInterval7, 200))
    assertThat(contains(intInterval7, 201))
    val intInterval8 = ValueInterval(100, 200, includeLower = false, includeUpper = false)
    assertThat(contains(intInterval8, null))
    assertThat(contains(intInterval8, 99))
    assertThat(contains(intInterval8, 100))
    assertThat(contains(intInterval8, 101)).isTrue
    assertThat(contains(intInterval8, 199)).isTrue
    assertThat(contains(intInterval8, 200))
    assertThat(contains(intInterval8, 201))
    // string type
    val strInterval5 = ValueInterval("m", "s", includeLower = true, includeUpper = true)
    assertThat(contains(strInterval5, null))
    assertThat(contains(strInterval5, "l"))
    assertThat(contains(strInterval5, "m")).isTrue
    assertThat(contains(strInterval5, "n")).isTrue
    assertThat(contains(strInterval5, "r")).isTrue
    assertThat(contains(strInterval5, "s")).isTrue
    assertThat(contains(strInterval5, "t"))
    val strInterval6 = ValueInterval("m", "s", includeLower = false, includeUpper = true)
    assertThat(contains(strInterval6, null))
    assertThat(contains(strInterval6, "l"))
    assertThat(contains(strInterval6, "m"))
    assertThat(contains(strInterval6, "n")).isTrue
    assertThat(contains(strInterval6, "r")).isTrue
    assertThat(contains(strInterval6, "s")).isTrue
    assertThat(contains(strInterval6, "t"))
    val strInterval7 = ValueInterval("m", "s", includeLower = true, includeUpper = false)
    assertThat(contains(strInterval7, null))
    assertThat(contains(strInterval7, "l"))
    assertThat(contains(strInterval7, "m")).isTrue
    assertThat(contains(strInterval7, "n")).isTrue
    assertThat(contains(strInterval7, "r")).isTrue
    assertThat(contains(strInterval7, "s"))
    assertThat(contains(strInterval7, "t"))
    val strInterval8 = ValueInterval("m", "s", includeLower = false, includeUpper = false)
    assertThat(contains(strInterval8, null))
    assertThat(contains(strInterval8, "l"))
    assertThat(contains(strInterval8, "m"))
    assertThat(contains(strInterval8, "n")).isTrue
    assertThat(contains(strInterval8, "r")).isTrue
    assertThat(contains(strInterval8, "s"))
    assertThat(contains(strInterval8, "t"))
  }

  @Test
  def testContainsTypeNotMatch(): Unit = {
    // LeftSemiInfiniteValueInterval
    val intInterval1 = ValueInterval(null, 100, includeLower = false, includeUpper = true)
    assertThat(contains(intInterval1, 1.0)).isTrue
    testContainsTypeNotMatch(intInterval1, "a")
    val strInterval1 = ValueInterval(null, "m", includeLower = false, includeUpper = true)
    testContainsTypeNotMatch(strInterval1, 1.0)
    testContainsTypeNotMatch(strInterval1, 1)
    // RightSemiInfiniteValueInterval
    val intInterval2 = ValueInterval(100, null, includeLower = true, includeUpper = false)
    assertThat(contains(intInterval2, 1.0))
    testContainsTypeNotMatch(intInterval2, "a")
    val strInterval2 = ValueInterval("m", null, includeLower = true, includeUpper = false)
    testContainsTypeNotMatch(strInterval2, 1.0)
    testContainsTypeNotMatch(strInterval2, 1)
    // FiniteValueInterval
    val intInterval3 = ValueInterval(100, 200, includeLower = true, includeUpper = true)
    assertThat(contains(intInterval3, 1.0))
    testContainsTypeNotMatch(intInterval3, "a")
    val strInterval3 = ValueInterval("m", "s", includeLower = true, includeUpper = true)
    testContainsTypeNotMatch(strInterval3, 1.0)
    testContainsTypeNotMatch(strInterval3, 1)
  }

  private def testContainsTypeNotMatch(interval: ValueInterval, value: Comparable[_]): Unit = {
    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => contains(interval, value))
  }
}
