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

package com.conductor.stream.utils.buffer;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BufferingIteratorTest {

    private BufferingIteratorImpl iterator;
    private Iterator mockIterator;

    @Before
    public void setup() {
        mockIterator = mock(Iterator.class);
        iterator = spy(new BufferingIteratorImpl(mockIterator));
    }

    @Test
    public void testHasNext() {
        when(mockIterator.hasNext())
                // first time
                .thenReturn(true)
                // second time
                .thenReturn(false);
        // iterator has next, so true
        assertTrue(iterator.hasNext());
        // iterator doesn't have next, and there are no items in the list, so false
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNext() {
        when(mockIterator.hasNext())
                // first time
                .thenReturn(true)
                // second time
                .thenReturn(false);
        when(mockIterator.next()).thenReturn(2);

        final List<Integer> next = iterator.next();
        assertEquals(1, next.size());
        assertEquals((Integer) 2, next.get(0));

        verify(iterator).setupState(2);
    }

    private class BufferingIteratorImpl extends BufferingIterator<Integer> {

        private boolean flush = false;

        BufferingIteratorImpl(Iterator<Integer> iterator) {
            super(iterator);
        }

        @Override
        boolean shouldFlush(Integer item) {
            return flush;
        }

        // Ghetto mock
        public void setFlush(boolean flush) {
            this.flush = flush;
        }
    }

}
