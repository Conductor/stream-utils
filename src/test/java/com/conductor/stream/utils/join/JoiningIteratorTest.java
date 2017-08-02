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

package com.conductor.stream.utils.join;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JoiningIteratorTest {

    private static final List<SideItem> JOIN_SIDE_1 = Arrays.asList(
            new SideItem(1, "one"),
            new SideItem(2, "two"),
            new SideItem(4, "four"),
            new SideItem(5, "five"),
            new SideItem(7, "seven"),
            new SideItem(8, "eight"),
            new SideItem(10, "ten")
    );

    private static final List<SideItem> JOIN_SIDE_2 = Arrays.asList(
            new SideItem(2, "'2'"),
            new SideItem(4, "'4'"),
            new SideItem(6, "'6'"),
            new SideItem(8, "'8'"),
            new SideItem(10, "'10'")
    );

    private static final Function<SideItem, Integer> KEYING_FUNCTION = (item) -> item == null ? null : item.getNumber();

    private static final BiFunction<SideItem, SideItem, String> JOIN_FUNCTION = (item1, item2) -> {
        if (item1 != null && item2 != null) {
            return String.format("%d - %s - %d - %s", item1.getNumber(), item1.getStringRepresentation(), item2.getNumber(), item2.getStringRepresentation());
        }
        else if (item1 == null) {
            return String.format("%d - %s", item2.getNumber(), item2.getStringRepresentation());
        }
        else {
            return String.format("%d - %s", item1.getNumber(), item1.getStringRepresentation());
        }
    };

    private static final BiFunction<SideItem, SideItem, SideItem> JOIN_RETURN_LHS_FUNCTION = (item1, item2) -> item1;

    /**
     * The following are basic tests of functionality for left, inner and outer joins
     */
    @Test
    public void testFullOuterJoin() {
        runTest(
                JOIN_SIDE_1.stream(),
                JOIN_SIDE_2.stream(),
                JOIN_FUNCTION,
                JoinType.OUTER,

                "1 - one",
                "2 - two - 2 - '2'",
                "4 - four - 4 - '4'",
                "5 - five",
                "6 - '6'",
                "7 - seven",
                "8 - eight - 8 - '8'",
                "10 - ten - 10 - '10'"
        );
    }

    @Test
    public void testFullOuterJoinOppositeSide() {
        runTest(
                JOIN_SIDE_2.stream(),
                JOIN_SIDE_1.stream(),
                JOIN_FUNCTION,
                JoinType.OUTER,

                "1 - one",
                "2 - '2' - 2 - two",
                "4 - '4' - 4 - four",
                "5 - five",
                "6 - '6'",
                "7 - seven",
                "8 - '8' - 8 - eight",
                "10 - '10' - 10 - ten"
        );
    }

    /**
     * This test may seem like overkill, but I want to verify
     * that we can get items one at a time.
     */
    @Test
    public void testFullOuterJoinStreamy() {
        final Iterator<String> iterator = new JoiningIterator<>(
                JOIN_SIDE_1.stream(),
                JOIN_SIDE_2.stream(),
                Comparator.naturalOrder(),
                KEYING_FUNCTION,
                KEYING_FUNCTION,
                JOIN_FUNCTION,
                JoinType.OUTER);

        assertEquals("1 - one", iterator.next());
        assertEquals("2 - two - 2 - '2'", iterator.next());
        assertEquals("4 - four - 4 - '4'", iterator.next());
        assertEquals("5 - five", iterator.next());
        assertEquals("6 - '6'", iterator.next());
        assertEquals("7 - seven", iterator.next());
        assertEquals("8 - eight - 8 - '8'", iterator.next());
        assertEquals("10 - ten - 10 - '10'", iterator.next());
    }

    @Test
    public void testLeftJoinWithInnerJoin() {
        final Iterator<SideItem> iterator = new JoiningIterator<>(
                JOIN_SIDE_1.stream(),
                JOIN_SIDE_2.stream(),
                Comparator.naturalOrder(),
                KEYING_FUNCTION,
                KEYING_FUNCTION,
                JOIN_RETURN_LHS_FUNCTION,
                JoinType.LEFT);

        runTest(
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false),
                JOIN_SIDE_1.stream(),
                JOIN_FUNCTION,
                JoinType.INNER,

                "1 - one - 1 - one",
                "2 - two - 2 - two",
                "4 - four - 4 - four",
                "5 - five - 5 - five",
                "7 - seven - 7 - seven",
                "8 - eight - 8 - eight",
                "10 - ten - 10 - ten"
        );
    }

    @Test
    public void testLeftJoin() {
        runTest(
                JOIN_SIDE_1.stream(),
                JOIN_SIDE_2.stream(),
                JOIN_FUNCTION,
                JoinType.LEFT,

                "1 - one",
                "2 - two - 2 - '2'",
                "4 - four - 4 - '4'",
                "5 - five",
                "7 - seven",
                "8 - eight - 8 - '8'",
                "10 - ten - 10 - '10'"
        );
    }

    @Test
    public void testLeftJoinOppositeSide() {
        runTest(
                JOIN_SIDE_2.stream(),
                JOIN_SIDE_1.stream(),
                JOIN_FUNCTION,
                JoinType.LEFT,

                "2 - '2' - 2 - two",
                "4 - '4' - 4 - four",
                "6 - '6'",
                "8 - '8' - 8 - eight",
                "10 - '10' - 10 - ten"
        );
    }

    @Test
    public void testFullInnerJoin() {
        runTest(
                JOIN_SIDE_1.stream(),
                JOIN_SIDE_2.stream(),
                JOIN_FUNCTION,
                JoinType.INNER,

                "2 - two - 2 - '2'",
                "4 - four - 4 - '4'",
                "8 - eight - 8 - '8'",
                "10 - ten - 10 - '10'"
        );
    }

    @Test
    public void testFullInnerJoinOppositeSide() {
        runTest(
                JOIN_SIDE_2.stream(),
                JOIN_SIDE_1.stream(),
                JOIN_FUNCTION,
                JoinType.INNER,

                "2 - '2' - 2 - two",
                "4 - '4' - 4 - four",
                "8 - '8' - 8 - eight",
                "10 - '10' - 10 - ten"
        );
    }


    /**
     * The following are tests where one of the streams never emits an item
     */
    @Test
    public void testLeftJoinWithEmptyStream() {
        runTest(
                JOIN_SIDE_1.stream(),
                Stream.empty(),
                JOIN_FUNCTION,
                JoinType.LEFT,

                "1 - one",
                "2 - two",
                "4 - four",
                "5 - five",
                "7 - seven",
                "8 - eight",
                "10 - ten"
        );

        runTest(
                Stream.empty(),
                JOIN_SIDE_1.stream(),
                JOIN_FUNCTION,
                JoinType.LEFT);
    }

    @Test
    public void testFullOuterJoinWithEmptyStream() {
        final String[] expected = new String[]{
                "1 - one",
                "2 - two",
                "4 - four",
                "5 - five",
                "7 - seven",
                "8 - eight",
                "10 - ten"
        };

        runTest(
                JOIN_SIDE_1.stream(),
                Stream.empty(),
                JOIN_FUNCTION,
                JoinType.OUTER,
                expected
        );

        runTest(
                Stream.empty(),
                JOIN_SIDE_1.stream(),
                JOIN_FUNCTION,
                JoinType.OUTER,
                expected
        );
    }

    @Test
    public void testInnerJoinWithEmptyStream() {
        runTest(
                JOIN_SIDE_1.stream(),
                Stream.empty(),
                JOIN_FUNCTION,
                JoinType.INNER);

        runTest(
                Stream.empty(),
                JOIN_SIDE_1.stream(),
                JOIN_FUNCTION,
                JoinType.INNER);
    }

    private void runTest(
            Stream<SideItem> stream1,
            Stream<SideItem> stream2,
            BiFunction<SideItem, SideItem, String> joinFunction,
            JoinType joinType,
            String... expected
    ) {
        final Iterator<String> iterator = new JoiningIterator<>(
                stream1,
                stream2,
                Comparator.nullsLast(Comparator.naturalOrder()),
                KEYING_FUNCTION,
                KEYING_FUNCTION,
                joinFunction,
                joinType);
        final List<String> output = new ArrayList<>();
        iterator.forEachRemaining(output::add);

        assertEquals(Arrays.asList(expected), output);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testJoinNextWithEmptyIteratorThrowsException() {
        final Iterator<String> iterator = new JoiningIterator<>(
                Stream.empty(),
                Stream.empty(),
                Comparator.nullsLast(Comparator.naturalOrder()),
                KEYING_FUNCTION,
                KEYING_FUNCTION,
                JOIN_FUNCTION,
                JoinType.LEFT);
        thrown.expect(NoSuchElementException.class);
        iterator.next();
    }

    @Test
    public void testJoinWithError() {
        Stream<SideItem> stream1 = Stream.of(
                new SideItem(1, "1l"),
                new SideItem(2, "2l"),
                new SideItem(3, "3l"),
                new SideItem(4, "4l"),
                new SideItem(5, "5l"),
                new SideItem(6, "6l")
        );
        Iterator<SideItem> iterator = mock(Iterator.class);
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.next())
                .thenReturn(new SideItem(1, "1r"))
                .thenThrow(new IllegalStateException("Holy exception batman!"))
                .thenReturn(new SideItem(5, "5r"));

        final Iterator<String> joined = new JoiningIterator<>(
                stream1,
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false),
                Comparator.nullsLast(Comparator.naturalOrder()),
                KEYING_FUNCTION,
                KEYING_FUNCTION,
                JOIN_FUNCTION,
                JoinType.LEFT);

        assertEquals(joined.next(), "1 - 1l - 1 - 1r");
        // this next call should throw
        thrown.expect(RuntimeException.class);
        joined.next();
    }

    @Test
    public void testJoinWithErrorInJoinFunction() throws Exception {
        Stream<SideItem> stream1 = Stream.of(
                new SideItem(1, "1l"),
                new SideItem(2, "2l"),
                new SideItem(3, "3l"),
                new SideItem(4, "4l"),
                new SideItem(5, "5l"),
                new SideItem(6, "6l")
        );
        Stream<SideItem> stream2 = Stream.of(
                new SideItem(1, "1r"),
                new SideItem(2, "2r"),
                new SideItem(3, "3r"),
                new SideItem(4, "4r"),
                new SideItem(5, "5r"),
                new SideItem(6, "6r")
        );

        final Iterator<String> joined = new JoiningIterator<>(
                stream1,
                stream2,
                Comparator.nullsLast(Comparator.naturalOrder()),
                KEYING_FUNCTION,
                KEYING_FUNCTION,
                (leftSide, rightSide) -> {
                    if (leftSide != null && leftSide.getNumber() == 3) {
                        throw new IllegalStateException("Holy exception batman!");
                    }
                    return JOIN_FUNCTION.apply(leftSide, rightSide);
                },
                JoinType.LEFT);

        assertEquals(joined.next(), "1 - 1l - 1 - 1r");
        assertEquals(joined.next(), "2 - 2l - 2 - 2r");
        // this next call should throw
        thrown.expect(RuntimeException.class);
        joined.next();
    }

    private static class SideItem {
        private final int number;
        private final String stringRepresentation;

        SideItem(int number, String stringRepresentation) {
            this.number = number;
            this.stringRepresentation = stringRepresentation;
        }

        int getNumber() {
            return number;
        }

        String getStringRepresentation() {
            return stringRepresentation;
        }
    }
}
