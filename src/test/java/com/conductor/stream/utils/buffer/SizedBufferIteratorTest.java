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

public class SizedBufferIteratorTest {

    private SizedBufferIterator<Integer> iterator;

    @Before
    public void setup() {
        iterator = new SizedBufferIterator<>(Stream.<Integer>empty().iterator(), 3);
    }

    @Test
    public void testShouldFlush() {
        // setup the state to be 0
        iterator.setupState(null);
        // should flush should now be false
        assertFalse(iterator.shouldFlush(null));
        // chuck another item in there, also shouldn't flush because size is 3 and this is item number 2
        assertFalse(iterator.shouldFlush(null));
        // now count is 3, so it should flush
        assertTrue(iterator.shouldFlush(null));
    }

    @Test
    public void testSetupStateResets() {
        // setup the state to be 0
        iterator.setupState(null);
        // should flush should now be false
        assertFalse(iterator.shouldFlush(null));
        // chuck another item in there, also shouldn't flush because size is 3 and this is item number 2
        assertFalse(iterator.shouldFlush(null));
        // setup state again
        iterator.setupState(null);
        // this is the third item, but we reset the state, so it should still not flush
        assertFalse(iterator.shouldFlush(null));
    }

}
