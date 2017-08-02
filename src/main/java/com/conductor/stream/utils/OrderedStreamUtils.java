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

package com.conductor.stream.utils;

import com.conductor.stream.utils.buffer.KeyedBufferIterator;
import com.conductor.stream.utils.join.JoinType;
import com.conductor.stream.utils.join.JoiningIterator;
import com.conductor.stream.utils.merge.SortedMergeIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class is a series of utilities specifically for dealing with
 * ordered streams. We use this because we stream through reports and
 * group/join them in ways that depend on ordering.
 *
 * @author Benjamin Shai
 */
public final class OrderedStreamUtils {

    private OrderedStreamUtils() {}

    /**
     * Java 8 streams don't support streaming groupings, just materialized grouping.
     * This groups a stream by a key, using the keying function provided.
     *
     * The stream must be sorted by the key for this to function properly.
     *
     * @param stream stream to group.
     * @param keyingFunction function to generate the key to be grouped by.
     * @return grouped stream.
     */
    public static <TYPE, KEY> Stream<List<TYPE>> groupBy(Stream<TYPE> stream, Function<TYPE, KEY> keyingFunction) {
        final Iterator<TYPE> iterator = stream.iterator();

        final Iterator<List<TYPE>> iter = new KeyedBufferIterator<>(iterator, keyingFunction);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false);
    }

    /**
     * This is a convenience wrapper around groupBy that also takes in an aggregation
     * function that turns the collection of items into an instance of a type.
     *
     * @param stream stream to group.
     * @param keyingFunction function to generate the key to be grouped by.
     * @param aggregationFunction function to apply to the group stream.
     * @return grouped value.
     */
    public static <TYPE, KEY, AGGREGATE> Stream<AGGREGATE> aggregate(Stream<TYPE> stream, Function<TYPE, KEY> keyingFunction, Function<List<TYPE>, AGGREGATE> aggregationFunction) {
        return groupBy(stream, keyingFunction)
                .map(aggregationFunction);
    }

    /**
     * Creates and returns a new Stream that merges together all the provided streams,
     * using the natural ordering of the items.
     *
     * As this lives in OrderedStreamUtils, this method assumes that the streams are
     * already each individually sorted by the natural ordering of the items. This just
     * zips them together.
     *
     * @param streams the streams to merge together.
     * @return the stream of all the items, in order.
     */
    public static <TYPE extends Comparable> Stream<TYPE> sortedMerge(List<Stream<TYPE>> streams) {
        return sortedMerge(streams, Comparator.naturalOrder());
    }

    /**
     * Creates and returns a new Stream that merges together all the provided streams,
     * using the provided comparator to compute the order of items.
     *
     * As this lives in OrderedStreamUtils, this method assumes that the streams are
     * already each individually sorted by the provided comparator. This just zips
     * them together.
     *
     * @param streams the streams to merge together.
     * @param comparator the comparator to use to merge the streams.
     * @return the stream of all the items, in order.
     */
    public static <TYPE> Stream<TYPE> sortedMerge(List<Stream<TYPE>> streams, Comparator<TYPE> comparator) {
        final Iterator<TYPE> iter = new SortedMergeIterator(streams, comparator);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false);
    }

    /**
     * Returns a stream that is a join of the two provided sorted streams. There are
     * three join types supported: full inner, full outer, and left. In case of duplicates,
     * the items will be joined in the order in which they appear.
     *
     * The inputted streams must each be sorted according to the key for this to function
     * properly.
     *
     * @param leftHandSide the left side stream to join.
     * @param rightHandSide the right side stream to join.
     * @param ordering a comparator which specifies the relative ordering of both streams.
     * @param leftHandKeyingFunction a function which returns a key value, used by the
     *                               comparator to determine the relative location in the stream.
     * @param rightHandKeyingFunction a function which returns a key value, used by the
     *                                comparator to determine the relative location in the stream.
     * @param joinFunction a function which join the values from the two streams.
     * @param joinType the type of join to perform -- left, outer, or inner.
     * @return the joined stream.
     */
    public static <KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> Stream<RESULT> join(
            final Stream<LEFT_VALUE> leftHandSide,
            final Stream<RIGHT_VALUE> rightHandSide,
            final Comparator<KEY> ordering,
            final Function<LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Function<RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final BiFunction<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final JoinType joinType
    ) {
        final JoiningIterator<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> iter = new JoiningIterator(leftHandSide, rightHandSide, ordering, leftHandKeyingFunction, rightHandKeyingFunction, joinFunction, joinType);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false);

    }

}
