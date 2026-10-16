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

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.compression.common.VarLongEncoding;
import org.neo4j.gds.degree.DegreeCentralityAlgorithmEstimateDefinition;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;

public final class Node2VecMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final Node2VecParameters parameters;

    public Node2VecMemoryEstimateDefinition(Node2VecParameters parameters) {
        this.parameters = parameters;
    }


    @Override
    public MemoryEstimation memoryEstimation() {
        int walksPerNode = parameters.samplingWalkParameters().walksPerNode();
        int walkLength = parameters.samplingWalkParameters().walkLength();
        int embeddingDimension = parameters.trainParameters().embeddingDimension();
        var builder =  MemoryEstimations.builder(Node2Vec.class)
            .add("compressed random walks", bytesCompressedRandomWalks(walksPerNode,walkLength))
            .add("probability cache", randomWalksMemoryEstimation())
            .add("model", modelMemoryEstimation(embeddingDimension))
            .perThread("walks", Estimate.sizeOfLongArray(walkLength));

        if (parameters.weighted()) {
            builder = builder.add("degree centrality",degreeCentralityMemoryEstimation());
        }
        return builder.build();
    }

    private MemoryEstimation degreeCentralityMemoryEstimation() {
            return new DegreeCentralityAlgorithmEstimateDefinition(true).memoryEstimation();
    }

    private MemoryEstimation bytesCompressedRandomWalks(int walksPerNode,int walkLength){
        return
            MemoryEstimations.builder()
                .perNode(
                    "random walks", (nodeCount) -> {
                        //Let's take an upperbound and assume every entry zig-zags between 0,and nodecount
                        // the deltas become  nodecount, - nodecount,
                        //2*nodeCount, because  `zigzag` encoding makes -a ~~  2|a|-1 and a ~~ 2|a|
                        var bytesPerEntry = VarLongEncoding.encodedVLongSize(2 * nodeCount);
                        long entries = ((long) walkLength) * bytesPerEntry;
                        var byteArraySize = Estimate.sizeOfByteArray(entries);
                        var numberOfRandomWalks = nodeCount * walksPerNode;
                        return HugeObjectArray.memoryEstimation(numberOfRandomWalks, byteArraySize);
                    }
                )
                .perNode(
                    "walk lengths", (nodeCount) -> {
                        var numberOfRandomWalks = nodeCount * walksPerNode;
                        return HugeIntArray.memoryEstimation(numberOfRandomWalks);
                    }
                ).perThread("iterator", Estimate.sizeOfLongArray(walkLength))
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
        var vectorMemoryEstimation = Estimate.sizeOfFloatArray(embeddingDimension);

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
