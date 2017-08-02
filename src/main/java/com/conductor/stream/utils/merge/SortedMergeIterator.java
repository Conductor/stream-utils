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

import com.conductor.stream.utils.misc.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This iterator does a sorted merge of many streams.
 *
 * It takes in a list of streams, and interleaves them.
 *
 * This operator assumes that the streams in question are all sorted according
 * to the same key, and that the comparator provided compares on the same
 * criteria. Basically, it takes the "smallest next item" from all of the
 * streams, and keeps doing this until all streams are empty.
 *
 * NOTE: this is NOT thread safe, as ordered streams cannot be parallel.
 *
 * @param <TYPE> the type of the items in each of the input streams.
 *
 * @author Benjamin Shai
 */
public class SortedMergeIterator<TYPE> implements Iterator<TYPE> {

    private final List<PeekingIterator<TYPE>> iterators;
    private final Comparator<TYPE> comparator;

    /**
     * Creates a new SortedMerge Iterator.
     *
     * @param streams a list of ordered streams to merge.
     * @param comparator the comparator that all those streams are sorted by.
     */
    public SortedMergeIterator(List<Stream<TYPE>> streams, Comparator<TYPE> comparator) {
        // take the full list of streams
        this.iterators = streams.stream()
                // turn them into iterators
                .map(BaseStream::iterator)
                // wrap them in peeking iterators
                .map(PeekingIterator::new)
                // collect that into a list
                .collect(Collectors.toList());
        this.comparator = comparator;
    }

    /**
     * Determines whether or not there are items left to emit.
     *
     * @return true or false.
     */
    @Override
    public boolean hasNext() {
        // as long as any of our iterators have anything available, we do too!
        return this.iterators.stream().anyMatch(PeekingIterator::hasNext);
    }

    /**
     * Gets the next item, according to the comparator.
     *
     * @return the next item.
     */
    @Override
    public TYPE next() {
        // Store a reference to the smallest item (out of all the next 1 items
        // per iterator). Initialize to null so the first call is guaranteed
        // to store its item.
        TYPE smallestItem = null;
        // Store a reference to the iterator containing the smallest item.
        PeekingIterator<TYPE> smallestIterator = null;

        // go through each iterator
        Iterator<PeekingIterator<TYPE>> listIterator = iterators.iterator();
        while(listIterator.hasNext()) {
            PeekingIterator<TYPE> iterator = listIterator.next();
            // if the iterator doesn't have any items left, skip it
            if (!iterator.hasNext()) {
                // for performance, we can remove this iterator from our list
                // so we no longer try to poll it
                listIterator.remove();
                continue;
            }
            // if smallest item is bigger than the item peeked out of the current
            // iterator (or smallest item is null because it's the first execution)...
            if (smallestItem == null || comparator.compare(smallestItem, iterator.peek()) > 0) {
                // update the reference to smallest item
                smallestItem = iterator.peek();
                // store the iterator containing the smallest item
                smallestIterator = iterator;
            }
        }

        // If we never found a "smallest iterator", that means that every iterator is empty.
        // In following the contract of `Iterator.next`, throw a NoSuchElementException.
        if (smallestIterator == null) {
            throw new NoSuchElementException();
        }

        // Consume the smallest item and return it
        return smallestIterator.next();
    }
}
