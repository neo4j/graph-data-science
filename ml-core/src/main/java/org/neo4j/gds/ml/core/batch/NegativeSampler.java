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
import org.neo4j.graphalgo.core.utils.partition.Partition;

import java.util.Random;

public class NegativeSampler {
    public static long negativeNode(Partition partition, Random rand, LongToDoubleFunction nodeWeightFn) {
        double randomValue = rand.nextDouble();
        double cumulativeProbability = 0;

        for (long nodeId = partition.startNode(); nodeId < partition.nodeCount(); nodeId++) {
            cumulativeProbability += nodeWeightFn.applyAsDouble(nodeId);
            if (randomValue < cumulativeProbability) {
                return nodeId;
            }
        }
        throw new RuntimeException(
            "This happens when there are no relationships in the Graph. " +
            "This condition is checked by the calling procedure."
        );
    }

}
