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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryUsage;

public final class Node2VecMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition<Node2VecBaseConfig> {
    @Override
    public MemoryEstimation memoryEstimation(Node2VecBaseConfig configuration) {
        int walksPerNode = configuration.walksPerNode();
        int walkLength = configuration.walkLength();
        int embeddingDimension = configuration.embeddingDimension();
        return MemoryEstimations.builder(Node2Vec.class)
            .perNode("random walks", (nodeCount) -> {
                var numberOfRandomWalks = nodeCount * walksPerNode;
                var randomWalkMemoryUsage = MemoryUsage.sizeOfLongArray(walkLength);
                return HugeObjectArray.memoryEstimation(numberOfRandomWalks, randomWalkMemoryUsage);
            })
            .add("probability cache", randomWalksMemoryEstimation())
            .add("model", modelMemoryEstimation(embeddingDimension))
            .build();
    }

    private MemoryEstimation randomWalksMemoryEstimation() {
        return MemoryEstimations.builder(RandomWalkProbabilities.class)
            .perNode("node frequencies", HugeLongArray::memoryEstimation)
            .perNode("positive sampling probabilities", HugeDoubleArray::memoryEstimation)
            .perNode("negative sampling distribution", HugeLongArray::memoryEstimation)
            .build();
    }


    private MemoryEstimation modelMemoryEstimation(int embeddingDimension) {
        var vectorMemoryEstimation = MemoryUsage.sizeOfFloatArray(embeddingDimension);

        return MemoryEstimations.builder(Node2VecModel.class)
            .perNode(
                "center embeddings",
                (nodeCount) -> HugeObjectArray.memoryEstimation(nodeCount, vectorMemoryEstimation)
            )
            .perNode(
                "context embeddings",
                (nodeCount) -> HugeObjectArray.memoryEstimation(nodeCount, vectorMemoryEstimation)
            )
            .build();
    }

}
