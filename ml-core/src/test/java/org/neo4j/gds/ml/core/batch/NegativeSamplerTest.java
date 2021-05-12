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
package org.neo4j.gds.ml.core.batch;

import org.eclipse.collections.api.block.function.primitive.LongToDoubleFunction;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.LongStream;

class NegativeSamplerTest {

    @Test
    void negativeSampling() {
        // trying to mimic the current behaviour in GraphSageModelTrainer
        var nodeCount = 10;
        var sampledNodes = HugeLongArray.newArray(nodeCount, AllocationTracker.empty());

        var random = new Random(42L);
        var degrees = random.ints(nodeCount, 1, nodeCount).toArray();

        var degreeProbabilityNormalizer = LongStream
            .range(0, nodeCount)
            .mapToDouble(nodeId -> Math.pow(degrees[(int) nodeId], 0.75))
            .sum();


        var samples = 200;
        var fictiveDegree = (LongToDoubleFunction) (nodeId) ->Math.pow(degrees[(int) nodeId], 0.75) / degreeProbabilityNormalizer;

        for (int i = 0; i < samples; i++) {
            var sample = NegativeSampler.negativeNode(Partition.of(0, nodeCount), random, fictiveDegree);
            sampledNodes.addTo(sample, 1);
        }

        System.out.println("degrees");
        System.out.println(Arrays.toString(degrees));
        System.out.println("samples");
        System.out.println(sampledNodes);
    }

}