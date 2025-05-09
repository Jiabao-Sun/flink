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

package org.apache.flink.table.runtime.operators.window.tvf.slicing;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.operators.window.MergeCallback;
import org.apache.flink.table.runtime.operators.window.tvf.common.ClockService;

import org.apache.flink.shaded.guava33.com.google.common.collect.Lists;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Utilities for testing {@link SliceAssigner}s. */
abstract class SliceAssignerTestBase {

    private static final ClockService CLOCK_SERVICE = System::currentTimeMillis;

    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    static Collection<ZoneId> zoneIds() {
        return Arrays.asList(ZoneId.of("America/Los_Angeles"), ZoneId.of("Asia/Shanghai"));
    }

    protected static void assertErrorMessage(Runnable runnable, String errorMessage) {
        assertThatThrownBy(runnable::run).hasMessageContaining(errorMessage);
    }

    protected static long assignSliceEnd(SliceAssigner assigner, long timestamp) {
        return assigner.assignSliceEnd(row(timestamp), CLOCK_SERVICE);
    }

    protected static List<Long> expiredSlices(SliceAssigner assigner, long sliceEnd) {
        return Lists.newArrayList(assigner.expiredSlices(sliceEnd));
    }

    protected static Long mergeResultSlice(SliceSharedAssigner assigner, long sliceEnd)
            throws Exception {
        TestingMergingCallBack callBack = new TestingMergingCallBack();
        assigner.mergeSlices(sliceEnd, callBack);
        return callBack.mergeResult;
    }

    protected static List<Long> toBeMergedSlices(SliceSharedAssigner assigner, long sliceEnd)
            throws Exception {
        TestingMergingCallBack callBack = new TestingMergingCallBack();
        assigner.mergeSlices(sliceEnd, callBack);
        return callBack.toBeMerged;
    }

    protected static RowData row(long timestamp) {
        BinaryRowData binaryRowData = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRowData);
        writer.writeTimestamp(0, TimestampData.fromEpochMillis(timestamp), 3);
        writer.complete();
        return binaryRowData;
    }

    private static final class TestingMergingCallBack
            implements MergeCallback<Long, Iterable<Long>> {

        private Long mergeResult;
        private List<Long> toBeMerged;

        @Override
        public void merge(Long mergeResult, Iterable<Long> toBeMerged) throws Exception {
            this.mergeResult = mergeResult;
            this.toBeMerged = Lists.newArrayList(toBeMerged);
        }
    }

    protected static void assertSliceStartEnd(
            String start, String end, long epochMills, SliceAssigner assigner) {

        assertThat(localTimestampStr(assigner.getWindowStart(assignSliceEnd(assigner, epochMills))))
                .isEqualTo(start);
        assertThat(localTimestampStr(assignSliceEnd(assigner, epochMills))).isEqualTo(end);
    }

    public static String localTimestampStr(long epochMills) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMills), UTC_ZONE_ID).toString();
    }

    /** Get utc mills from a timestamp string and the parameterized time zone. */
    protected long utcMills(String timestampStr) {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampStr);
        return localDateTime.atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
    }

    /** Get local mills from a timestamp string and the parameterized time zone. */
    protected long localMills(String timestampStr, ZoneId shiftTimeZone) {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampStr);
        return localDateTime.atZone(shiftTimeZone).toInstant().toEpochMilli();
    }
}
