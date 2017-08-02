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

import java.util.Iterator;

/**
 * This iterator should split the provided iterator into lists
 * of the provided size.
 *
 * @param <TYPE> the type of the items in the underlying iterator.
 *
 * @author Benjamin Shai
 */
public class SizedBufferIterator<TYPE> extends BufferingIterator<TYPE> {

    private int currentItem;
    private final int size;

    /**
     * Creates an iterator.
     *
     * @param iterator underlying iterator.
     * @param size desired size of each list of items to be emitted.
     */
    public SizedBufferIterator(Iterator<TYPE> iterator, int size) {
        super(iterator);
        this.size = size;
    }

    /**
     * Sets up the state such that the current size is 0.
     *
     * @param firstItem the very first item in the iterator, for setting up state.
     */
    @Override
    void setupState(TYPE firstItem) {
        currentItem = 0;
    }

    /**
     * Flush if the max list size has been hit.
     *
     * @param item the current item to determine whether flush should occur.
     * @return true or false.
     */
    @Override
    boolean shouldFlush(TYPE item) {
        // increment the current item count, and get the modulus of size
        currentItem = ++currentItem % size;
        // if the modulus is 0, we've hit our max, and we flush the list
        return currentItem == 0;
    }
}
