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

package org.neo4j.graphalgo.core.utils;

import java.util.ArrayList;
import java.util.Collection;

public class PartitionUtils {

    public interface TaskProducer<T extends Runnable> {
        T produce(long batchStart, long batchEnd);
    }

    public static <T extends Runnable> Collection<T> numberAlignedPartitioning(
        TaskProducer<T> taskSupplier,
        int concurrency,
        long nodeCount,
        long alignTo
    ) {
        final long initialBatchSize = ParallelUtil.adjustedBatchSize(nodeCount, concurrency, alignTo);
        final long remainder = initialBatchSize % alignTo;
        final long adjustedBatchSize = remainder == 0 ? initialBatchSize : initialBatchSize + (alignTo - remainder);

        Collection<T> tasks = new ArrayList<>(concurrency);
        for (long i = 0; i < nodeCount; i+=adjustedBatchSize) {
            tasks.add(taskSupplier.produce(i, Math.min(nodeCount, i + adjustedBatchSize - 1)));
        }

        return tasks;
    }

}
