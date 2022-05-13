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
package org.neo4j.gds.embeddings.graphsage;

import com.carrotsearch.hppc.LongHashSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.ImmutableRelationshipCursor;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.ml.core.samplers.WeightedUniformSampler;
import org.neo4j.gds.ml.core.subgraph.NeighborhoodSampler;

import java.util.Arrays;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

public final class GraphSageBatchSampler {

    public static final double DEGREE_SMOOTHING_FACTOR = 0.75;

    private GraphSageBatchSampler() {}

    /**
     * For each node in the batch we sample one neighbor node and one negative node from the graph.
     */
    static long[] sampleNeighborAndNegativeNodePerBatchNode(Partition batch, Graph localGraph, int searchDepth, long randomSeed) {
        var neighbours = neighborBatch(localGraph, batch, randomSeed, searchDepth).toArray();

        LongStream negativeSamples = negativeBatch(localGraph, Math.toIntExact(batch.nodeCount()), neighbours, randomSeed);

        return LongStream.concat(
            batch.stream(),
            LongStream.concat(
                Arrays.stream(neighbours),
                // batch.nodeCount is <= config.batchsize (which is an int)
                negativeBatch(localGraph, Math.toIntExact(batch.nodeCount()), neighbours, randomSeed)
            )
        ).toArray();
    }

    static LongStream neighborBatch(Graph graph, Partition batch, long batchLocalSeed, int searchDepth) {
        var neighborBatchBuilder = LongStream.builder();
        var localRandom = new Random(batchLocalSeed);

        // sample a neighbor for each batchNode
        batch.consume(nodeId -> {
            // randomWalk with at most maxSearchDepth steps and only save last node
            int actualSearchDepth = localRandom.nextInt(searchDepth) + 1;
            AtomicLong currentNode = new AtomicLong(nodeId);
            while (actualSearchDepth > 0) {
                NeighborhoodSampler neighborhoodSampler = new NeighborhoodSampler(currentNode.get() + actualSearchDepth);
                OptionalLong maybeSample = neighborhoodSampler.sampleOne(graph, nodeId);
                if (maybeSample.isPresent()) {
                    currentNode.set(maybeSample.getAsLong());
                } else {
                    // terminate
                    actualSearchDepth = 0;
                }
                actualSearchDepth--;
            }
            neighborBatchBuilder.add(currentNode.get());
        });

        return neighborBatchBuilder.build();
    }

    // get a negative sample per node in batch
    static LongStream negativeBatch(Graph graph, int batchSize, long[] batchNeighbors, long batchLocalRandomSeed) {
        long nodeCount = graph.nodeCount();
        var sampler = new WeightedUniformSampler(batchLocalRandomSeed);

        // avoid sampling the sampled neighbor as a negative example
        var neighborsSet = new LongHashSet(batchNeighbors.length);
        neighborsSet.addAll(batchNeighbors);

        // each node should be possible to sample
        // therefore we need fictive rels to all nodes
        // Math.log to avoid always sampling the high degree nodes
        var degreeWeightedNodes = LongStream.range(0, nodeCount)
            .mapToObj(nodeId -> ImmutableRelationshipCursor.of(0, nodeId, Math.pow(graph.degree(nodeId),
                DEGREE_SMOOTHING_FACTOR
            )));

        return sampler.sample(degreeWeightedNodes, nodeCount, batchSize, sample -> !neighborsSet.contains(sample));
    }
}
