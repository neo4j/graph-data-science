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

import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import java.util.function.Function;

public class FractionSplitter {

    public static MemoryEstimation estimate(double trainFraction) {
        return MemoryEstimations.builder(FractionSplitter.class.getSimpleName())
            .perNode("train", (n) -> HugeLongArray.memoryEstimation(trainSize(n, trainFraction)))
            .perNode("test", (n) -> HugeLongArray.memoryEstimation(n - trainSize(n, trainFraction)))
            .build();
    }

    private static long trainSize(long nodeCount, double trainFraction) {
        return (long) (nodeCount * trainFraction);
    }

    public TrainingExamplesSplit split(ReadOnlyHugeLongArray ids, double trainFraction) {
        long trainSize = trainSize(ids.size(), trainFraction);
        long testSize = ids.size() - trainSize;
        var train = initHLA(trainSize, ids::get);
        var test = initHLA(testSize, i -> ids.get(i + trainSize));
        return TrainingExamplesSplit.of(train, test);
    }

    private ReadOnlyHugeLongArray initHLA(long size, Function<Long, Long> transform) {
        var array = HugeLongArray.newArray(size);
        array.setAll(transform::apply);
        return ReadOnlyHugeLongArray.of(array);
    }

}
