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
package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/** Tests for TimeIndicatorRelDataType. */
class TimeIndicatorRelDataTypeTest {

  @Test
  def testGenerateTypeString() {
    val typeFactory = new FlinkTypeFactory(
      classOf[TimeIndicatorRelDataTypeTest].getClassLoader,
      FlinkTypeSystem.INSTANCE)

    assertThat(typeFactory.createProctimeIndicatorType(false).getFullTypeString)
      .isEqualTo("TIMESTAMP(3) *PROCTIME* NOT NULL")
    assertThat(typeFactory.createRowtimeIndicatorType(false, false).getFullTypeString)
      .isEqualTo("TIMESTAMP_LTZ(3) *ROWTIME* NOT NULL")
    assertThat(typeFactory.createRowtimeIndicatorType(false, true).getFullTypeString)
  }

}
