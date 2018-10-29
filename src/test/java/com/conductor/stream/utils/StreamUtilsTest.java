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

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class StreamUtilsTest {

    @Test
    public void testBuffer() {

        Stream<Integer> stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8);

        List<List<Integer>> expectedOutcome = Arrays.asList(
                Arrays.asList(1, 2, 3),
                Arrays.asList(4, 5, 6),
                Arrays.asList(7, 8));

        assertEquals(expectedOutcome, StreamUtils.buffer(stream, 3).collect(Collectors.toList()));
    }

    /**
     * This test is proves that the streamy way works.
     */
    @Test(timeout = 1000)
    public void testBufferStreamy() {
        // Create a mock supplier, that way we know how many times get was called
        Supplier<Integer> supplier = Mockito.mock(Supplier.class);
        Mockito.when(supplier.get()).thenReturn(0);

        final Stream<Integer> infiniteStream = Stream.generate(supplier);
        final Stream<List<Integer>> groupedStream = StreamUtils.buffer(infiniteStream.limit(1000000000), 10);
        assertEquals(groupedStream.findFirst().get(), Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0, 0, 0));

        // get should only be called 11 times because we're buffering 10, and we retrieve the
        // 11th to do the check that we're over the limit
        Mockito.verify(supplier, Mockito.times(11)).get();
    }

    @Test
    public void testNoCombiner() {
        String combined = Stream.of(1, 2, 3)
                .collect(
                        StringBuilder::new,
                        StringBuilder::append,
                        StreamUtils.noCombiner()
                ).toString();
        assertEquals("123", combined);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNoCombinerErrors() {
        final BiConsumer<Integer, Integer> combiner = StreamUtils.noCombiner();
        combiner.accept(1, 2);
    }

    @Test
    public void testSwitchIfEmpty() {
        final Stream<Integer> integerStream = StreamUtils.switchIfEmpty(Stream.empty(), () -> Stream.of(1, 2, 3));
        assertEquals(Arrays.asList(1, 2, 3), integerStream.collect(Collectors.toList()));
    }

    @Test
    public void testSwitchIfEmpty_endlessLoop() {
        final Stream<Integer> integerStream = StreamUtils.switchIfEmpty(Stream.empty(), () -> Stream.of(1));
        assertEquals(Collections.singletonList(1), integerStream.collect(Collectors.toList()));
    }

    @Test
    public void testNestedSwitchIfEmpty() {
        final Stream<Integer> integerStream =
                StreamUtils.switchIfEmpty(Stream.empty(),
                        () -> StreamUtils.switchIfEmpty(Stream.empty(), () -> Stream.of(1, 2, 3)));
        assertEquals(Arrays.asList(1, 2, 3), integerStream.collect(Collectors.toList()));
    }

    @Test
    public void testSwitchIfEmptyNotEmpty() {
        final Stream<Integer> integerStream = StreamUtils.switchIfEmpty(Stream.of(1, 2, 3), () -> Stream.of(4, 5, 6));
        assertEquals(Arrays.asList(1, 2, 3), integerStream.collect(Collectors.toList()));
    }

    @Test
    public void testSwitchIfEmptyBothEmpty() {
        final Stream<Integer> integerStream = StreamUtils.switchIfEmpty(Stream.empty(), Stream::empty);
        assertEquals(new ArrayList(), integerStream.collect(Collectors.toList()));
    }
}
