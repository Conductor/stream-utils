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

import com.conductor.stream.utils.buffer.SizedBufferIterator;
import com.conductor.stream.utils.misc.SwitchIfEmptySpliterator;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class will be the interface for using all these utilities.
 *
 * @author Benjamin Shai
 */
public final class StreamUtils {

    private StreamUtils() {}

    /**
     * Java 8 streams don't support streaming groupings, just materialized grouping.
     * This chunks a stream into lists the provided size.
     *
     * @param stream stream to group.
     * @param size size of the lists to emit.
     * @param <TYPE> the type of items in the stream.
     * @return grouped stream.
     */
    public static <TYPE> Stream<List<TYPE>> buffer(Stream<TYPE> stream, final int size) {
        final Iterator<TYPE> iterator = stream.iterator();

        final Iterator<List<TYPE>> iter = new SizedBufferIterator(iterator, size);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false);
    }

    // Just create one BiConsumer that is a no combiner, and reuse it.
    private static final BiConsumer NO_COMBINER = (t1, t2) -> {
        throw new UnsupportedOperationException("No combiner supplied for merging parallel results");
    };

    /**
     * This is a utility function to get lambda to be passed into a collector, which
     * enforced that the combine method should NOT be called.
     *
     * @param <TYPE> the type of item in the BiConsumer.
     * @return a BiConsumer of your type for collecting/reducing.
     */
    @SuppressWarnings("unchecked")
    public static <TYPE> BiConsumer<TYPE, TYPE> noCombiner() {
        return NO_COMBINER;
    }

    /**
     * This is a convenience method that is similar to RxJava's similarly named
     * switchIfEmpty. It allows you to pass in a supplier of a replacement stream
     * that will be used if the provided stream is empty. A supplier is used so
     * that the replacement stream is only evaluated if the given stream is
     * empty.
     *
     * @param stream the original stream.
     * @param replacementStreamSupplier the supplier that provides the alternate stream.
     * @param <TYPE> the type of items in the stream.
     * @return stream.
     */
    public static <TYPE> Stream<TYPE> switchIfEmpty(
            Stream<TYPE> stream, Supplier<Stream<TYPE>> replacementStreamSupplier) {

        return StreamSupport.stream(new SwitchIfEmptySpliterator<>(stream, replacementStreamSupplier), false);
    }

}
