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

package com.conductor.stream.utils.misc;

import org.junit.Before;
import org.junit.Test;

import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SwitchIfEmptySpliteratorTest {

    private Supplier<Stream<Integer>> supplier;
    private Spliterator<Integer> mockSpliterator;
    private SwitchIfEmptySpliterator<Integer> spliterator;

    @Before
    public void setup() {
        Stream<Integer> stream = mock(Stream.class);
        supplier = mock(Supplier.class);
        mockSpliterator = mock(Spliterator.class);

        when(stream.spliterator()).thenReturn(mockSpliterator);

        spliterator = new SwitchIfEmptySpliterator<>(stream, supplier);
    }

    @Test
    public void tryAdvanceEmptyStream() throws Exception {
        when(mockSpliterator.tryAdvance(null)).thenReturn(false);
        Stream<Integer> stream = mock(Stream.class);
        when(supplier.get()).thenReturn(stream);
        final Spliterator mock = mock(Spliterator.class);
        when(stream.spliterator()).thenReturn(mock);

        spliterator.tryAdvance(null);

        verify(supplier).get();
        verify(stream).spliterator();
        verify(mock).tryAdvance(null);
    }

    @Test
    public void tryAdvanceNonEmptyStream() throws Exception {
        when(mockSpliterator.tryAdvance(null)).thenReturn(true);

        assertTrue(spliterator.tryAdvance(null));

        verifyNoMoreInteractions(supplier);
    }

    @Test
    public void tryAdvanceEventuallyEmptyStream() throws Exception {
        when(mockSpliterator.tryAdvance(null))
                .thenReturn(true)
                .thenReturn(false);

        assertTrue(spliterator.tryAdvance(null));
        assertFalse(spliterator.tryAdvance(null));

        verifyNoMoreInteractions(supplier);
    }

    @Test
    public void trySplit() throws Exception {
        // this should call the delegate
        spliterator.trySplit();
        verify(mockSpliterator).trySplit();
    }

    @Test
    public void estimateSize() throws Exception {
        // this should call the delegate
        spliterator.estimateSize();
        verify(mockSpliterator).estimateSize();
    }

    @Test
    public void characteristics() throws Exception {
        // this should call the delegate
        spliterator.characteristics();
        verify(mockSpliterator).characteristics();
    }

}
