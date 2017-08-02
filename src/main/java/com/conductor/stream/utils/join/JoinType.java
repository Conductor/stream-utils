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

/**
 * Supported join types. The join type is the definition of
 * the expected behavior of the join.
 *
 * @author Benjamin Shai
 */
public enum JoinType {

    /**
     * This join only joins items that are matched on both
     * sides. If the key can only be found on one side, that
     * item will be discarded.
     */
    INNER,

    /**
     * This join joins all items, no matter what. If a matching
     * item cannot be found on either side, null will be passed
     * in for the missing item.
     */
    OUTER,

    /**
     * Similar to a SQL left join, this join will take every
     * single item from the left side of the join. If a matching
     * item cannot be found on the right side, null will be
     * passed in. If an item is found on the right that is not
     * present on the left, it will be discarded.
     *
     * You may notice that there is no RIGHT join. That's
     * because you can just use this left join but reverse the
     * order of the streams.
     */
    LEFT

}
