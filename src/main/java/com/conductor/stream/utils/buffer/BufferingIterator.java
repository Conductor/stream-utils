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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This is an iterator that support buffering an iterator into lists of items.
 * I decided to make it abstract to allow for extensions of it to save state.
 *
 * Implement the method `shouldFlush` (and optionally `setupState`) to use
 * this.
 *
 * @param <TYPE> the type of item in the underlying iterator.
 *
 * @author Benjamin Shai
 */
abstract class BufferingIterator<TYPE> implements Iterator<List<TYPE>> {

    private final Iterator<TYPE> iterator;
    private final List<TYPE> list;
    private boolean isFirst;

    /**
     * Create a buffering iterator, passing in the underlying iterator
     * to be buffered.
     *
     * @param iterator the iterator to buffer
     */
    BufferingIterator(Iterator<TYPE> iterator) {
        this.iterator = iterator;
        list = new ArrayList<>();
        isFirst = true;
    }

    /**
     * Whether or not there is a list of items left.
     *
     * @return true or false.
     */
    @Override
    public boolean hasNext() {
        return iterator.hasNext() || !list.isEmpty();
    }

    /**
     * Get the next item (which is a list of items from the underlying
     * iterator).
     *
     * @return a list of items.
     */
    @Override
    public List<TYPE> next() {
        // if the iterator has items in it, process them
        while (iterator.hasNext()) {
            // get the current item
            final TYPE next = iterator.next();

            // Setup any necessary initial state if it's the very first
            // request we're receiving.
            if (isFirst) {
                isFirst = false;
                setupState(next);
                // still add the item to the list
                list.add(next);

                // continue, because until we hit the flush point, we haven't
                // returned a list.
                continue;
            }

            // Check if we should flush the list and start a new one.
            if (shouldFlush(next)) {
                // flush list
                final List<TYPE> retList = flushList();
                // start new list
                list.add(next);

                return retList;
            }

            // Otherwise, we must not be in flush mode. Just add it
            // to the list and carry on.
            list.add(next);
        }

        // otherwise, send the last list down the line
        return flushList();
    }

    /**
     * This should be used if there is any setup needed on the first item.
     *
     * It's a noop by default, override it if you need it.
     *
     * @param firstItem the very first item in the iterator, for setting up state.
     */
    void setupState(TYPE firstItem) { /* noop */ };

    /**
     * This method determines whether we should flush the list we're holding
     * down the stream.
     *
     * @param item the current item to determine whether flush should occur.
     * @return whether or not we should flush the list.
     */
    abstract boolean shouldFlush(TYPE item);

    /**
     * This method fills up a list with all the values currently being
     * held on to, and clears the current list pointer so it can start
     * holding the next set of items.
     *
     * @return old list
     */
    private List<TYPE> flushList() {
        List<TYPE> oldList = new ArrayList<>(list);
        list.clear();
        return oldList;
    }
}
