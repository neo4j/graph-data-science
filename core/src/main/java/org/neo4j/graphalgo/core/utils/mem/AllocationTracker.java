/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;

public interface AllocationTracker extends Supplier<String> {

    AllocationTracker EMPTY = new AllocationTracker() {
        @Override
        public void add(long delta) {
        }

        @Override
        public void remove(long delta) {
        }

        @Override
        public long tracked() {
            return 0L;
        }

        @Override
        public String get() {
            return "";
        }

        @Override
        public String getUsageString() {
            return "";
        }

        @Override
        public String getUsageString(String label) {
            return "";
        }
    };

    static boolean isTracking(@Nullable AllocationTracker tracker) {
        return tracker != null && tracker != EMPTY;
    }

    static AllocationTracker create() {
        return InMemoryAllocationTracker.create();
    }

    void add(long delta);

    void remove(long delta);

    long tracked();

    default String getUsageString() {
        return humanReadable(tracked());
    }

    default String getUsageString(String label) {
        return label + getUsageString();
    }

    @Override
    default String get() {
        return getUsageString("Memory usage: ");
    }
}
