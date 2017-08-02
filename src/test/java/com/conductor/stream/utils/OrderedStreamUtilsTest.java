/*
 * Copyright 2017 Conductor, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.conductor.stream.utils;

import com.conductor.stream.utils.join.JoinType;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class OrderedStreamUtilsTest {

    public static final TestRecord VAL_1 = new TestRecord(1, "val1");
    public static final TestRecord VAL_2 = new TestRecord(1, "val2");
    public static final TestRecord VAL_3 = new TestRecord(1, "val3");
    public static final TestRecord VAL_4 = new TestRecord(2, "val1");
    public static final TestRecord VAL_5 = new TestRecord(2, "val2");
    public static final TestRecord VAL_6 = new TestRecord(3, "val1");
    public static final TestRecord VAL_7 = new TestRecord(4, "val1");
    public static final TestRecord VAL_8 = new TestRecord(4, "val2");

    @Test
    public void testGroupBy() {

        Stream<TestRecord> stream = getRecordStream();

        List<List<TestRecord>> expectedOutcome = Arrays.asList(
                Arrays.asList(VAL_1, VAL_2, VAL_3),
                Arrays.asList(VAL_4, VAL_5),
                Arrays.asList(VAL_6),
                Arrays.asList(VAL_7, VAL_8));

        assertEquals(expectedOutcome, OrderedStreamUtils.groupBy(stream, r -> r.getId()).collect(Collectors.toList()));
    }

    /**
     * This test is ignored, because it proves that java 8 collectors
     * materialize whole sets. See the next test.
     */
    @Test(timeout = 500)
    @Ignore
    public void testGroupByNonStreamyShitty() {
        final Stream<Integer> infiniteStream = Stream.iterate(0, i -> i + 1);
        // This is an infinite stream containing all numbers. We're going to
        // chunk it up into batches of 10, and see if we can get the JVM to
        // explode.
        final Map<Integer, List<Integer>> collect = infiniteStream
                .limit(1000000000)
                .collect(Collectors.groupingBy(i -> i / 10));

        assertEquals(collect.get(0), Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    /**
     * This test is proves that the streamy way works. Observe how it doesn't
     * time out like the above test does.
     */
    @Test(timeout = 500)
    public void testGroupByStreamy() {
        final Stream<Integer> infiniteStream = Stream.iterate(0, i -> i + 1);
        // This is an infinite stream containing all numbers. We're going to
        // chunk it up into batches of 10, and see if we can get the JVM to
        // explode.
        final Stream<List<Integer>> groupedStream = OrderedStreamUtils.groupBy(infiniteStream.limit(1000000000), i -> i / 10);

        assertEquals(groupedStream.findFirst().get(), Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    /**
     * This test validates that we don't get stack overflow exceptions.
     */
    @Test
    @Ignore
    public void testNoStackOverflow() {
        final Stream<Integer> infiniteStream = Stream.iterate(0, i -> i + 1);
        // This is an infinite stream containing all numbers. We're going
        // to group all of them together, and see if the stack explodes.
        final Stream<List<Integer>> groupedStream = OrderedStreamUtils.groupBy(infiniteStream.limit(100000), i -> 1);

        assertEquals(100000, groupedStream.findFirst().get().size());
    }

    @Test
    public void testAggregate() {
        OrderedStreamUtils.aggregate(Stream.of(1, 3, 5, 2, 4), (i) -> i % 2, List::toString);
        Stream<TestRecord> stream = getRecordStream();

        Stream<String> expectedOutcome = Stream.of(
                "1val1val2val3",
                "2val1val2",
                "3val1",
                "4val1val2"
        );

        assertEquals(expectedOutcome.collect(Collectors.toList()),
                OrderedStreamUtils.aggregate(
                        stream,
                        r -> r.getId(),
                        records -> records.get(0).getId() + records.stream().map(r -> r.getValue()).collect(Collectors.joining(""))
                ).collect(Collectors.toList())
        );
    }

    @Test
    public void testSymmetricMerge() {
        final Stream<Integer> s1 = Stream.of(1, 3, 5, 7);
        final Stream<Integer> s2 = Stream.of(2, 4, 6, 8);

        final Stream<Integer> stream = OrderedStreamUtils.sortedMerge(Arrays.asList(s1, s2));

        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), stream.collect(Collectors.toList()));
    }

    @Test
    public void testCustomComparator() {
        final Stream<Integer> s1 = Stream.of(1, 3, 5, 7);
        final Stream<Integer> s2 = Stream.of(2, 4, 6, 8);

        final Stream<Integer> stream = OrderedStreamUtils.sortedMerge(Arrays.asList(s1, s2), Comparator.<Integer>naturalOrder().reversed());

        // What's happening here is the opposite integer comparison is being applied.
        // Instead of trying to find the lower of the two numbers, it finds the higher.
        // That results in the weird ordering seen.
        assertEquals(Arrays.asList(2, 4, 6, 8, 1, 3, 5, 7), stream.collect(Collectors.toList()));
    }

    /*
      This is just a single test that tests the static utility method. For more
      comprehensive tests, check out JoiningIteratorTest.
     */
    @Test
    public void testJoin() {
        final Stream<Integer> join = OrderedStreamUtils.join(
                Stream.of("1", "2", "3", "4", "5"),
                Stream.of(1, 2, 3, 4, 5),
                Comparator.naturalOrder(),
                Integer::parseInt,
                Function.identity(),
                (left, right) -> Integer.parseInt(left) + right,
                JoinType.OUTER
        );
        assertEquals(Arrays.asList(2, 4, 6, 8, 10), join.collect(Collectors.toList()));
    }

    private Stream<TestRecord> getRecordStream() {
        return Stream.of(
                VAL_1,
                VAL_2,
                VAL_3,
                VAL_4,
                VAL_5,
                VAL_6,
                VAL_7,
                VAL_8
        );
    }
}
