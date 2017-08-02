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

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * This is a spliterator to be used to provide an alternate stream, if the first stream
 * is empty. It is named similarly to RxJava's SwitchIfEmpty operator.
 * 
 * Vaguely lifted from Holger's post on Stack Overflow (http://stackoverflow.com/a/26659678)
 *
 * @author Benjamin Shai
 */
public class SwitchIfEmptySpliterator<TYPE> implements Spliterator<TYPE> {
    
    private Spliterator<TYPE> streamSpliterator;
    private final Supplier<Stream<TYPE>> alternateStreamSupplier;

    private boolean seen;

    /**
     * Creates the spliterator.
     *
     * @param stream the stream to inspect.
     * @param alternateStreamSupplier a lazy supplier to retrieve the alternate stream.
     */
    public SwitchIfEmptySpliterator(Stream<TYPE> stream, Supplier<Stream<TYPE>> alternateStreamSupplier) {
        this.streamSpliterator = stream.spliterator();
        this.alternateStreamSupplier = alternateStreamSupplier;
    }

    /**
     * Attempt to advance the spliterator. If this is the first call, and there
     * are no items in the stream, defer to the alternate.
     *
     * @param action the action we're trying to do.
     * @return whether or not we were able to advance the stream.
     */
    public boolean tryAdvance(Consumer<? super TYPE> action) {
        // Try to advance the stream
        boolean advanced = streamSpliterator.tryAdvance(action);

        // If we've never seen an item, and we fail to advance the
        // stream, it must be empty.
        if (!seen && !advanced) {
            // replace the stream with the replacement.
            streamSpliterator = alternateStreamSupplier.get().spliterator();
            // now we use the replacement stream's tryAdvance call
            return streamSpliterator.tryAdvance(action);
        }
        seen = true;
        return advanced;
    }

    /**
     * Try to split the stream. Defers to the underlying stream.
     *
     * @return a split spliterator.
     */
    public Spliterator<TYPE> trySplit() {
        return streamSpliterator.trySplit();
    }

    /**
     * Estimates the size of the stream. Defers to the underlying stream.
     *
     * @return the approximate size.
     */
    public long estimateSize() {
        return streamSpliterator.estimateSize();
    }

    /**
     * Obtains the characteristics of the stream. Defers to the underlying stream.
     *
     * @return the characteristics, packed into an int.
     */
    public int characteristics() {
        return streamSpliterator.characteristics();
    }

}
