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

import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyedBufferIteratorTest {

    private KeyedBufferIterator<Integer, Integer> iterator;

    @Before
    public void setup() {
        iterator = new KeyedBufferIterator<>(
                // stream doesn't matter because we're only testing the
                // specific functions which don't use the iterator
                Stream.<Integer>empty().iterator(),
                // uneven split of the stream
                (i) -> i == 1 || i == 2 ? 1 : 2
        );
    }

    @Test
    public void setupState() throws Exception {
        // set the state to be 1
        iterator.setupState(1);
        // now it shouldn't flush anything that resolves to 1
        assertFalse(iterator.shouldFlush(1));
        assertFalse(iterator.shouldFlush(2));
    }

    @Test
    public void shouldFlush() throws Exception {
        // set the state to be 1
        iterator.setupState(1);
        // now it shouldn't flush anything that resolves to 1
        assertFalse(iterator.shouldFlush(1));
        assertTrue(iterator.shouldFlush(5));
        // now that it flushed a 5, it shouldn't flush 5s any more until the current key changes
        assertFalse(iterator.shouldFlush(5));
        assertTrue(iterator.shouldFlush(1));
    }

}
