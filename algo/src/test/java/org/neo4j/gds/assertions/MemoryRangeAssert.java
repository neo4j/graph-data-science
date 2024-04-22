/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.assertions;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.LongAssert;
import org.neo4j.gds.mem.MemoryRange;

public final class MemoryRangeAssert extends AbstractAssert<MemoryRangeAssert, MemoryRange> {

    private MemoryRangeAssert(MemoryRange memoryRange) {
        super(memoryRange, MemoryRangeAssert.class);
    }

    public static MemoryRangeAssert assertThat(MemoryRange actual) {
        return new MemoryRangeAssert(actual);
    }

    public MemoryRangeAssert hasSameMinAndMax() {
        isNotNull();

        if (actual.min != actual.max) {
            failWithMessage(
                "Expected `min` and `max` to have the same value but they were: min = %s, max = %s",
                actual.min,
                actual.max
            );
        }

        return this;
    }

    public MemoryRangeAssert hasSameMinAndMaxEqualTo(long expected) {

        hasSameMinAndMax();

        if(actual.min != expected) {
            failWithMessage(
                "Expected `min` and `max` to be `%s` but it was `%s`",
                expected,
                actual.min
            );

        }

        return this;
    }

    public MemoryRangeAssert hasMin(long min) {
        isNotNull();

        if (actual.min != min) {
            failWithMessage("Expected `min` to be `%s` but it was `%s`.", min, actual.min);
        }

        return this;
    }

    public MemoryRangeAssert hasMax(long max) {
        isNotNull();

        if (actual.max != max) {
            failWithMessage("Expected `max` to be `%s` but it was `%s`.", max, actual.max);
        }

        return this;
    }

    public LongAssert min() {
        isNotNull();
        return new LongAssert(actual.min);
    }

    public LongAssert max() {
        isNotNull();
        return new LongAssert(actual.max);
    }
}
