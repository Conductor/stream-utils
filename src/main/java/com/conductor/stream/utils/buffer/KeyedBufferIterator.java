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
import java.util.function.Function;

/**
 * This is an implementation of BufferingIterator that groups streams by
 * a key which can be obtained by the keying function provided.
 *
 * IMPORTANT: the stream must be ordered.
 *
 * @param <TYPE> the type of item in the underlying iterator.
 * @param <KEY> the type of the item returned by the keying function.
 *
 * @author Benjamin Shai
 */
public class KeyedBufferIterator<TYPE, KEY> extends BufferingIterator<TYPE> {

    private KEY currentKey;
    private Function<TYPE, KEY> keyingFunction;

    /**
     * Creates an iterator.
     *
     * @param iterator the underlying iterator.
     * @param keyingFunction the keying function to determine grouping.
     */
    public KeyedBufferIterator(Iterator<TYPE> iterator, Function<TYPE, KEY> keyingFunction) {
        super(iterator);
        this.keyingFunction = keyingFunction;
    }

    /**
     * Sets up the iterator with a reference to the key of the first item.
     *
     * @param firstItem the very first item in the iterator, for setting up state.
     */
    @Override
    void setupState(TYPE firstItem) {
        // save the state of the current item
        currentKey = keyingFunction.apply(firstItem);
    }

    /**
     * Determines whether the given item matches the current key.
     *
     * @param item the current item to determine whether flush should occur.
     * @return true or false.
     */
    @Override
    boolean shouldFlush(TYPE item) {
        final KEY key = keyingFunction.apply(item);
        // if there's a key match
        if (!currentKey.equals(key)) {
            // save the new key
            currentKey = key;
            // flush the list
            return true;
        }
        return false;
    }

}
