/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.mem;

import java.util.Objects;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;

/**
 * Represents a range of positive byte values.
 * The range can span 0 bytes when the min and max value are identical.
 * The range can represent 0 bytes when the min and max values are both 0.
 */
public final class MemoryRange {

    private static final MemoryRange NULL_RANGE = new MemoryRange(0L, 0L);

    public static MemoryRange of(final long value) {
        return of(value, value);
    }

    public static MemoryRange of(final long min, final long max) {
        if (min == 0 && max == 0) {
            return NULL_RANGE;
        }
        if (min < 0) {
            throw new IllegalArgumentException("min range < 0: " + min);
        }
        if (max < 0) {
            throw new IllegalArgumentException("max range < 0: " + max);
        }
        if (max < min) {
            throw new IllegalArgumentException("max range < min: " + max + " < " + min);
        }
        return new MemoryRange(min, max);
    }

    public static MemoryRange empty() {
        return NULL_RANGE;
    }

    public final long min;
    public final long max;

    private MemoryRange(final long min, final long max) {
        this.min = min;
        this.max = max;
    }

    public MemoryRange add(final MemoryRange other) {
        if (this.isEmpty()) {
            return other;
        }
        if (other.isEmpty()) {
            return this;
        }
        long newMin = Math.addExact(this.min, other.min);
        long newMax = Math.addExact(this.max, other.max);
        if (newMin == min && newMax == max) {
            return this;
        }
        return new MemoryRange(newMin, newMax);
    }

    public MemoryRange times(final long count) {
        if (this.isEmpty() || count == 1L) {
            return this;
        }
        if (count == 0L) {
            return NULL_RANGE;
        }
        long newMin = Math.multiplyExact(this.min, count);
        long newMax = Math.multiplyExact(this.max, count);
        return new MemoryRange(newMin, newMax);
    }

    public boolean isEmpty() {
        return this == NULL_RANGE || (min == 0 && max == 0);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final MemoryRange range = (MemoryRange) o;
        return min == range.min &&
                max == range.max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }

    @Override
    public String toString() {
        if (min == max) {
            return humanReadable(min);
        } else {
            return "[" + humanReadable(min) + " ... " + humanReadable(max) + "]";
        }
    }
}
