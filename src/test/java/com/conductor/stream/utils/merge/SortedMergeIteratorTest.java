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

package com.conductor.stream.utils.merge;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SortedMergeIteratorTest {

    @Test
    public void testSymmetricMerge() {
        final Stream<Integer> s1 = Stream.of(1, 3, 5, 7);
        final Stream<Integer> s2 = Stream.of(2, 4, 6, 8);

        final Iterator<Integer> iterator = new SortedMergeIterator<>(Arrays.asList(s1, s2), Comparator.naturalOrder());
        final List<Integer> output = new ArrayList<>();
        iterator.forEachRemaining(output::add);

        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), output);
    }

    @Test
    public void testAsymmetricMerge() {
        final Stream<Integer> s1 = Stream.of(1, 3, 5, 7);
        final Stream<Integer> s2 = Stream.of(2, 4);

        final Iterator<Integer> iterator = new SortedMergeIterator<>(Arrays.asList(s1, s2), Comparator.naturalOrder());
        final List<Integer> output = new ArrayList<>();
        iterator.forEachRemaining(output::add);

        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 7), output);
    }

    @Test
    public void testEmptyEmpty() {
        final Stream<Integer> s1 = Stream.empty();
        final Stream<Integer> s2 = Stream.empty();

        final Iterator<Integer> iterator = new SortedMergeIterator<>(Arrays.asList(s1, s2), Comparator.naturalOrder());

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testFirstEmpty() {
        final Stream<Integer> s1 = Stream.of(1, 3, 5, 7);
        final Stream<Integer> s2 = Stream.empty();

        final Iterator<Integer> iterator = new SortedMergeIterator<>(Arrays.asList(s1, s2), Comparator.naturalOrder());
        final List<Integer> output = new ArrayList<>();
        iterator.forEachRemaining(output::add);

        assertEquals(Arrays.asList(1, 3, 5, 7), output);
    }

    @Test
    public void testSecondEmpty() {
        final Stream<Integer> s1 = Stream.empty();
        final Stream<Integer> s2 = Stream.of(1, 3, 5, 7);

        final Iterator<Integer> iterator = new SortedMergeIterator<>(Arrays.asList(s1, s2), Comparator.naturalOrder());
        final List<Integer> output = new ArrayList<>();
        iterator.forEachRemaining(output::add);

        assertEquals(Arrays.asList(1, 3, 5, 7), output);
    }

    @Test(expected = NoSuchElementException.class)
    public void testThrowsExceptionIfNextCalledOnEmptyStream() {
        final Stream<Integer> s1 = Stream.empty();
        final Stream<Integer> s2 = Stream.empty();

        final Iterator<Integer> iterator = new SortedMergeIterator<>(Arrays.asList(s1, s2), Comparator.naturalOrder());

        iterator.next();
    }

    @Test
    public void testEqualStreamsMerge() {
        final Stream<Integer> s1 = Stream.of(1, 2, 3, 4);
        final Stream<Integer> s2 = Stream.of(1, 2, 3, 4);

        final Iterator<Integer> iterator = new SortedMergeIterator<>(Arrays.asList(s1, s2), Comparator.naturalOrder());
        final List<Integer> output = new ArrayList<>();
        iterator.forEachRemaining(output::add);

        assertEquals(Arrays.asList(1, 1, 2, 2, 3, 3, 4, 4), output);
    }

    @Test
    public void testStreamWithNull() {
        final Stream<Integer> s1 = Stream.of(1, null, 3, 4);
        final Stream<Integer> s2 = Stream.of(1, 2, 3, 4);

        final Iterator<Integer> iterator = new SortedMergeIterator<>(Arrays.asList(s1, s2), Comparator.naturalOrder());
        final List<Integer> output = new ArrayList<>();
        iterator.forEachRemaining(output::add);

        assertEquals(Arrays.asList(1, 1, 2, 3, 4, null, 3, 4), output);
    }

    @Test(expected = IllegalStateException.class)
    public void testSameStreamDoesNothing() {
        final Stream<Integer> s1 = Stream.of(4, 3, 2, 1);

        // This will throw an illegal state exception, because when you try to merge
        // a stream with itself, it will try to close the stream twice, and then get
        // pissed that you're trying to close a stream that is already closed. I hope
        // this is ok with you, @arivas...
        new SortedMergeIterator<>(Arrays.asList(s1, s1), Comparator.naturalOrder());
    }

}
