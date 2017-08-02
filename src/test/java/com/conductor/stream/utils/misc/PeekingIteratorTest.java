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

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PeekingIteratorTest {

    private Iterator<Integer> mockIterator;
    private PeekingIterator<Integer> peekingIterator;

    @Before
    public void setup() {
        mockIterator = mock(Iterator.class);
        peekingIterator = new PeekingIterator<>(mockIterator);
    }

    @Test
    public void testHasNext() {
        when(mockIterator.hasNext()).thenReturn(true).thenReturn(false);
        when(mockIterator.next()).thenReturn(1);
        assertTrue(peekingIterator.hasNext());
        peekingIterator.next();
        assertFalse(peekingIterator.hasNext());

    }

    @Test
    public void testPeek() {
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenReturn(1);
        assertTrue(peekingIterator.hasNext());
        assertEquals(1, (long) peekingIterator.peek());
    }

    @Test(expected = NoSuchElementException.class)
    public void throwsOnPeekWithoutHasNext() {
        peekingIterator.peek();
    }

    @Test
    public void testNext() {
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenReturn(1);
        assertTrue(peekingIterator.hasNext());
        assertEquals(1, (long) peekingIterator.next());
    }

    @Test(expected = NoSuchElementException.class)
    public void throwsOnNextWithoutHasNext() {
        when(mockIterator.hasNext()).thenReturn(false);
        peekingIterator.next();
    }

    @Test
    public void doesNotThrowExceptionUntilNextCall() {
        // this test validates that if attempting to retrieve an item throws
        // an exception, that exception is not thrown until the appropriate
        // next/peek call happens.
        when(mockIterator.hasNext()).thenReturn(true).thenThrow(new RuntimeException());
        when(mockIterator.next()).thenReturn(1).thenThrow(new RuntimeException());
        assertTrue(peekingIterator.hasNext());
        assertEquals(1, (long) peekingIterator.peek());
        assertEquals(1, (long) peekingIterator.next());
        // now the next call should throw exceptions
        boolean caught = false;
        try {
            peekingIterator.hasNext();
        } catch(Exception e) {
            caught = true;
        }
        assertTrue(caught);
        caught = false;
        try {
            peekingIterator.peek();
        } catch(Exception e) {
            caught = true;
        }
        assertTrue(caught);
        caught = false;
        try {
            peekingIterator.next();
        } catch(Exception e) {
            caught = true;
        }
        assertTrue(caught);
    }

    @Test
    public void canCallPeekOrNextWithoutHasNextAsLongAsThereAreItemsLeft() {
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenReturn(1);
        assertEquals(1, (long) peekingIterator.peek());
        assertEquals(1, (long) peekingIterator.next());
    }
}
