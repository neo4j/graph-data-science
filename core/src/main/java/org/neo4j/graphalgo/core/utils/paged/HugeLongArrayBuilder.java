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
package org.neo4j.graphalgo.core.utils.paged;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.loading.InternalIdMappingBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

public final class HugeLongArrayBuilder extends HugeArrayBuilder<long[], HugeLongArray> implements InternalIdMappingBuilder<HugeArrayBuilder.BulkAdder<long[]>> {

    public static HugeLongArrayBuilder of(long length, AllocationTracker tracker) {
        HugeLongArray array = HugeLongArray.newArray(length, tracker);
        return new HugeLongArrayBuilder(array, length);
    }

    private HugeLongArrayBuilder(HugeLongArray array, final long length) {
        super(array, length);
    }

    @Override
    public @Nullable HugeLongArrayBuilder.BulkAdder<long[]> allocate(int batchLength) {
        return this.allocate((long) batchLength);
    }
}
