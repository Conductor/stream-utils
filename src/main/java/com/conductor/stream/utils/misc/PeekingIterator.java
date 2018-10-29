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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is an iterator that supports "peeking" at the next element, without
 * officially popping it off the iterator.
 *
 * It does this by always holding an item in memory, and when asked for the
 * next item, it replaces that item with a new one, and then returns the
 * newly released old current item.
 *
 * @param <TYPE>
 *
 * @author Benjamin Shai
 */
public class PeekingIterator<TYPE> implements Iterator<TYPE> {
    private final Iterator<TYPE> delegateIterator;
    private AtomicReference<TYPE> nextObject;
    // the exception caught from the last iteration
    private RuntimeException caughtException;

    public PeekingIterator(Iterator<TYPE> delegateIterator) {
        this.delegateIterator = delegateIterator;
    }

    /**
     * Check if there are any items left.
     *
     * @return true if there are, false if not.
     */
    @Override
    public boolean hasNext() {
        // If the last iteration threw an exception,
        // throw that exception now
        if (caughtException != null) {
            throw caughtException;
        }
        // If there is no nextObject, that means either
        // this is the first iteration, or the delegate
        // is empty. Either way, try to get the next item,
        // and use that for the null check below.
        if (this.nextObject == null) {
            // Cycle the next item.
            this.nextObject = getNextObject();
        }
        // as long as we currently hold on to an item, or
        // we hold on to an exception, we coo'
        return this.nextObject != null || this.caughtException != null;
    }

    /**
     * Get the next item, if such a thing is available, holding
     * on to any thrown exceptions for later use.
     *
     * @return a reference wrapping the object, or null if there's
     * nothing left.
     */
    private AtomicReference<TYPE> getNextObject() {
        // wrap all of this in a try/catch, because any of the calls
        // on the delegate may throw unchecked exceptions.
        try {
            // only try to get the next item if there is one.
            if (delegateIterator.hasNext()) {
                // Now that we know there's an item available, get it
                return new AtomicReference<>(delegateIterator.next());
            }
        } catch (RuntimeException e) {
            // if we catch an exception, hold onto it. We won't throw now,
            // because the user requested the previous item.
            caughtException = e;
        }
        return null;
    }

    /**
     * Get the next item, consuming it.
     *
     * @return the next item.
     */
    @Override
    public TYPE next() {
        // If the last iteration threw an exception,
        // throw that exception now
        if (caughtException != null) {
            throw caughtException;
        }
        // If the current item is null, this can happen for
        // two reasons.
        //   1. hasNext hasn't been called yet.
        //   2. the delegate is empty
        // in the first case, hasNext will make sure we have
        // an item to use, and in the second case hasNext
        // will return false
        if (this.nextObject == null && !this.hasNext()) {
            // In keeping with the signature of Iterator's
            // `next` method, if the delegate iterator is all
            // out, and we don't have a current item, then we
            // must throw a no such element exception.
            throw new NoSuchElementException();
        }

        // Store a local reference to the current item
        AtomicReference<TYPE> flush = this.nextObject;
        // replace the "current item" reference with the
        // nextObject item
        this.nextObject = getNextObject();
        // return our item
        return flush.get();
    }

    /**
     * Take a look at the next item without consuming it.
     *
     * @return the next item.
     */
    public TYPE peek() {
        // If the last iteration threw an exception,
        // throw that exception now
        if (caughtException != null) {
            throw caughtException;
        }
        if (nextObject == null && !hasNext()) {
            // if this has been called without first calling hasNext,
            // nextObject can be empty, and we throw an exception.
            throw new NoSuchElementException();
        }
        return nextObject.get();
    }
}
