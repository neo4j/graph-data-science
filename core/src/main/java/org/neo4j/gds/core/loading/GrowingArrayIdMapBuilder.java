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
package org.neo4j.gds.core.loading;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;

import java.util.concurrent.atomic.AtomicLong;

public final class GrowingArrayIdMapBuilder implements IdMapBuilder {

    private final HugeLongArrayBuilder arrayBuilder;
    private final AtomicLong allocationIndex;
    private final AutoCloseableThreadLocal<HugeLongArrayBuilder.Allocator> allocators;

    public static GrowingArrayIdMapBuilder of() {
        HugeLongArrayBuilder array = HugeLongArrayBuilder.newBuilder();
        return new GrowingArrayIdMapBuilder(array);
    }

    private GrowingArrayIdMapBuilder(HugeLongArrayBuilder arrayBuilder) {
        this.arrayBuilder = arrayBuilder;
        this.allocationIndex = new AtomicLong();
        this.allocators = AutoCloseableThreadLocal.withInitial(HugeLongArrayBuilder.Allocator::new);
    }

    @Override
    public @NotNull HugeLongArrayBuilder.Allocator allocate(int batchLength) {
        long startIndex = allocationIndex.getAndAdd(batchLength);

        HugeLongArrayBuilder.Allocator allocator = allocators.get();
        arrayBuilder.allocate(startIndex, batchLength, allocator);

        return allocator;
    }

    @Override
    public IdMap build(
        LabelInformation.Builder labelInformationBuilder,
        long highestNodeId,
        Concurrency concurrency
    ) {
        allocators.close();
        long nodeCount = size();
        var graphIds = this.arrayBuilder.build(nodeCount);
        return ArrayIdMapBuilderOps.build(graphIds, nodeCount, labelInformationBuilder, highestNodeId, concurrency);
    }

    public HugeLongArray array() {
        return arrayBuilder.build(size());
    }

    public long size() {
        return allocationIndex.get();
    }
}
