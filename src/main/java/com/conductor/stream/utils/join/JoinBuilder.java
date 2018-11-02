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

import java.util.Comparator;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This is a builder for the JoinIterator, to make construction
 * much simpler.
 *
 * @author Benjamin Shai
 */
public class JoinBuilder<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> {

    private Stream<LEFT_VALUE> leftHandSide;
    private Stream<RIGHT_VALUE> rightHandSide;
    private Comparator<KEY> ordering;
    private Function<LEFT_VALUE, KEY> leftHandKeyingFunction;
    private Function<RIGHT_VALUE, KEY> rightHandKeyingFunction;
    private BiFunction<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction;
    private JoinType joinType;

    public JoinBuilder() {}

    public JoinBuilder<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> setLeftHandSide(final Stream<LEFT_VALUE> leftHandSide) {
        this.leftHandSide = leftHandSide;
        return this;
    }

    /**
     * Getter so that we can appropriately clean up resources.
     *
     * @return the base stream on the left side
     */
    public Stream<LEFT_VALUE> getLeftHandSide() {
        return leftHandSide;
    }

    public JoinBuilder<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> setRightHandSide(final Stream<RIGHT_VALUE> rightHandSide) {
        this.rightHandSide = rightHandSide;
        return this;
    }

    /**
     * Getter so that we can appropriately clean up resources.
     *
     * @return the base stream on the right side
     */
    public Stream<RIGHT_VALUE> getRightHandSide() {
        return rightHandSide;
    }

    public JoinBuilder<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> setOrdering(final Comparator<KEY> ordering) {
        this.ordering = ordering;
        return this;
    }

    public JoinBuilder<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> setLeftHandKeyingFunction(final Function<LEFT_VALUE, KEY> leftHandKeyingFunction) {
        this.leftHandKeyingFunction = leftHandKeyingFunction;
        return this;
    }

    public JoinBuilder<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> setRightHandKeyingFunction(final Function<RIGHT_VALUE, KEY> rightHandKeyingFunction) {
        this.rightHandKeyingFunction = rightHandKeyingFunction;
        return this;
    }

    public JoinBuilder<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> setJoinFunction(final BiFunction<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction) {
        this.joinFunction = joinFunction;
        return this;
    }

    public JoinBuilder<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> setJoinType(final JoinType joinType) {
        this.joinType = joinType;
        return this;
    }

    public JoiningIterator<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> build() {
        Objects.requireNonNull(leftHandSide, "Left-hand-side stream must not be null.");
        Objects.requireNonNull(rightHandSide, "Right-hand-side stream must not be null.");
        Objects.requireNonNull(ordering, "Ordering comparator must not be null.");
        Objects.requireNonNull(leftHandKeyingFunction, "Left-hand-side keying function must not be null.");
        Objects.requireNonNull(rightHandKeyingFunction, "Right-hand-side keying function must not be null.");
        Objects.requireNonNull(joinFunction, "Join function must not be null.");
        Objects.requireNonNull(joinType, "Join type must not be null.");

        return new JoiningIterator<>(
                leftHandSide,
                rightHandSide,
                ordering,
                leftHandKeyingFunction,
                rightHandKeyingFunction,
                joinFunction,
                joinType
        );
    }

    /**
     * Static convenience method.
     *
     * @return a builder.
     */
    public static <KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> JoinBuilder<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> builder() {
        return new JoinBuilder<>();
    }
}
