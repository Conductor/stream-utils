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

package com.conductor.stream.utils.join;

import com.conductor.stream.utils.misc.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This is an iterator that joins two streams of ordered data.
 * That's right, ordered data. If your data isn't ordered...
 * don't use this please.
 *
 * As such, this iterator shouldn't be used in parallel streams,
 * as this iterator isn't fully thread safe.
 *
 * @param <KEY> the type of the key object. This key is used to
 *              determine equality of items in the join, as well
 *              as comparing the two sides to see which has the
 *              next item.
 * @param <LEFT_VALUE> the type of the items in the left hand
 *                     side iterator.
 * @param <RIGHT_VALUE> the type of the items in the right hand
 *                      side iterator.
 * @param <RESULT> the type of the items in the resulting iterator.
 *
 * @author Benjamin Shai
 */
public class JoiningIterator<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> implements Iterator<RESULT> {

    private PeekingIterator<LEFT_VALUE> leftHandSide;
    private PeekingIterator<RIGHT_VALUE> rightHandSide;
    private final Comparator<KEY> ordering;
    private final Function<LEFT_VALUE, KEY> leftHandKeyingFunction;
    private final Function<RIGHT_VALUE, KEY> rightHandKeyingFunction;
    private final BiFunction<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction;
    private final JoinType joinType;

    // Since we don't know if we have a next item for the hasNext call
    // until we actually try to join it (we can't poll the two sides
    // for their hasNext-ability because what if it's a left join and
    // we only have items in the right? what if it's an inner join and
    // we only have full streams of non-matching items?) so we cache
    // the next item in every hasNext call, and return it in the next
    // call. This is that cached item.
    private AtomicReference<RESULT> nextItem;

    /**
     * Creates a new joining iterator.
     *
     * NOTE - your comparator must be able to handle null values. Your join
     * function needs to if you're using any join type other than inner.
     *
     * @param leftHandSide the left side stream to join.
     * @param rightHandSide the right side stream to join.
     * @param ordering a comparator which specifies the relative ordering
     *                 of both streams.
     * @param leftHandKeyingFunction a function which returns a key value,
     *                               used by the comparator to determine the
     *                               relative location in the stream.
     * @param rightHandKeyingFunction a function which returns a key value,
     *                                used by the comparator to determine the
     *                                relative location in the stream.
     * @param joinFunction a function which join the values from the two streams.
     * @param joinType the type of join to perform -- left, outer, or inner.
     */
    public JoiningIterator(
            Stream<LEFT_VALUE> leftHandSide,
            Stream<RIGHT_VALUE> rightHandSide,
            Comparator<KEY> ordering,
            Function<LEFT_VALUE, KEY> leftHandKeyingFunction,
            Function<RIGHT_VALUE, KEY> rightHandKeyingFunction,
            BiFunction<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            JoinType joinType) {
        this.leftHandSide = new PeekingIterator<>(leftHandSide.iterator());
        this.rightHandSide = new PeekingIterator<>(rightHandSide.iterator());
        this.ordering = ordering;
        this.leftHandKeyingFunction = leftHandKeyingFunction;
        this.rightHandKeyingFunction = rightHandKeyingFunction;
        this.joinFunction = joinFunction;
        this.joinType = joinType;
    }

    /**
     * Determines whether there are items left to emit.
     *
     * @return true or false.
     */
    @Override
    public boolean hasNext() {
        this.nextItem = getNextJoinedItem();
        // if the item is null, we must have exhausted our supply of
        // items. If not, we're good for the next next call.
        return this.nextItem != null;
    }

    /**
     * Obtains the next joined item, applying the joining function.
     *
     * @return the item.
     */
    @Override
    public RESULT next() {
        // if someone called next without calling hasNext, we won't
        // have a nextItem. Attempt to get the nextItem, following
        // the contract of Iterator (throw an exception if there
        // isn't one).
        if (this.nextItem == null && !this.hasNext()) {
            throw new NoSuchElementException();
        }
        // now we need to null out nextItem so that people can't call
        // next twice without hasNext and get the same item.
        final RESULT returnItem = this.nextItem.get();
        this.nextItem = null;
        return returnItem;
    }

    /**
     * Computes the actual join, and finds the next appropriate
     * joined item.
     *
     * @return the joined item (which may be null) wrapped in a
     * reference, to protect against null items being improperly
     * handled, or null if no next item can be found.
     */
    private AtomicReference<RESULT> getNextJoinedItem() {
        // Get the next available items. Peek so we don't consume.
        // At any time, one side can be empty. Make sure to check for
        // hasNext first.
        boolean leftHasNext = leftHandSide.hasNext();
        boolean rightHasNext = rightHandSide.hasNext();

        while (leftHasNext || rightHasNext) {
            // Use an atomic reference because either side of the stream
            // can return null as an item, but there might be items left
            // in the stream.
            final LEFT_VALUE left = leftHasNext ? leftHandSide.peek() : null;
            final RIGHT_VALUE right = rightHasNext ? rightHandSide.peek() : null;

            final KEY leftHandKey = left == null ? null : leftHandKeyingFunction.apply(left);
            final KEY rightHandKey = right == null ? null : rightHandKeyingFunction.apply(right);

            // Depending on the join type, we have different criteria for
            // how we join (and whether we can join). The one thing in
            // common is that if the items are equal, always join them.
            if (ordering.compare(leftHandKey, rightHandKey) == 0) {
                // apply the join function and consume the item.
                return new AtomicReference<>(joinFunction.apply(leftHandSide.next(), rightHandSide.next()));
            }

            // now we know the keys aren't equal. That means we've got an
            // uneven join. We need to decide which side to poll to look
            // for the matching item, and decide what to do with the
            // unmatched items. That will depend on the join type.

            // if the left side item is smaller than the right side item,
            // then roll the left looking for a matching item for the
            // right side.
            else if (ordering.compare(leftHandKey, rightHandKey) < 0) {
                // we know the left is smaller. If the join type is left
                // or outer, we want the left item alone
                if (joinType == JoinType.OUTER || joinType == JoinType.LEFT) {
                    return new AtomicReference<>(joinFunction.apply(leftHandSide.next(), null));
                } else {
                    // if it's an inner join, we need to discard the left
                    // item, which is smaller, and try again.
                    leftHandSide.next();
                }
            }

            // if the right side item is smaller than the left side item,
            // then roll the right looking for a matching item for the
            // left side.
            else {
                // we know the right is smaller. If the join type is outer,
                // we want the right item alone
                if (joinType == JoinType.OUTER) {
                    return new AtomicReference<>(joinFunction.apply(null, rightHandSide.next()));
                } else {
                    // if it's an inner or left join, we need to discard
                    // the right item, which is smaller, and try again.
                    rightHandSide.next();
                }
            }
            // if we've made it this far without finding a match,
            // rev the hasNexts and try again
            leftHasNext = leftHandSide.hasNext();
            rightHasNext = rightHandSide.hasNext();
        }
        // if we somehow exited the loop without a result, we must
        // have exhausted both streams without finding a match.
        // return null in that case.
        return null;
    }
}
