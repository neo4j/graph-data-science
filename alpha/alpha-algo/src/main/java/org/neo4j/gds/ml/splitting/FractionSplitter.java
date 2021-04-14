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
package org.neo4j.gds.ml.splitting;

import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.function.Function;

public class FractionSplitter {

    private final AllocationTracker allocationTracker;

    static MemoryEstimation estimate(double trainFraction) {
        return MemoryEstimations.builder(FractionSplitter.class)
            .perNode("train", (n) -> HugeLongArray.memoryEstimation(trainSize(n, trainFraction)))
            .perNode("test", (n) -> HugeLongArray.memoryEstimation(n - trainSize(n, trainFraction)))
            .build();
    }

    private static long trainSize(long nodeCount, double trainFraction) {
        return (long) (nodeCount * trainFraction);
    }

    public FractionSplitter(AllocationTracker allocationTracker) {
        this.allocationTracker = allocationTracker;
    }

    public NodeSplit split(HugeLongArray ids, double trainFraction) {
        long trainSize = trainSize(ids.size(), trainFraction);
        long testSize = ids.size() - trainSize;
        var train = initHLA(trainSize, ids::get);
        var test = initHLA(testSize, i -> ids.get(i + trainSize));
        return NodeSplit.of(train, test);
    }

    private HugeLongArray initHLA(long size, Function<Long, Long> transform) {
        var array = HugeLongArray.newArray(size, allocationTracker);
        array.setAll(transform::apply);
        return array;
    }

}
