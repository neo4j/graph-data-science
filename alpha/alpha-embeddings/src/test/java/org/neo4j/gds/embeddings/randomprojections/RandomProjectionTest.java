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
package org.neo4j.gds.embeddings.randomprojections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RandomProjectionTest extends AlgoTestBase {

    static final RandomProjectionBaseConfig DEFAULT_CONFIG = ImmutableRandomProjectionBaseConfig.builder()
        .embeddingSize(128)
        .maxIterations(1)
        .build();

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node2)" +
        ", (d:Isolated)" +
        ", (e:Isolated)" +
        ", (a)-[:REL {weight: 2.0}]->(b)" +
        ", (b)-[:REL {weight: 1.0}]->(a)" +
        ", (a)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(a)" +
        ", (b)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(b)";

    @BeforeEach
    void setupGraphDb() {
        runQuery(DB_CYPHER);
    }

    @Test
    void shouldSwapInitialRandomVectors() {
        GraphLoader graphLoader = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Node1")
            .build();

        Graph graph = graphLoader.graph();

        RandomProjection randomProjection = new RandomProjection(
            graph,
            DEFAULT_CONFIG,
            progressLogger,
            AllocationTracker.empty()
        );

        randomProjection.initRandomVectors();
        HugeObjectArray<float[]> randomVectors = HugeObjectArray.newArray(float[].class, 2, AllocationTracker.empty());
        randomProjection.currentEmbedding(-1).copyTo(randomVectors, 2);
        randomProjection.propagateEmbeddings();
        HugeObjectArray<float[]> embeddings = randomProjection.embeddings();

        boolean isEqual = true;
        for (int i = 0; i < 128; i++) {
            isEqual &= embeddings.get(0)[i] == randomVectors.get(1)[i];
        }
        assertTrue(isEqual);
    }

    @Test
    void shouldAverageNeighbors() {
        GraphLoader graphLoader = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Node1")
            .addNodeLabel("Node2")
            .build();

        Graph graph = graphLoader.graph();

        RandomProjection randomProjection = new RandomProjection(
            graph,
            DEFAULT_CONFIG,
            progressLogger,
            AllocationTracker.empty()
        );

        randomProjection.initRandomVectors();
        HugeObjectArray<float[]> randomVectors = HugeObjectArray.newArray(float[].class, 3, AllocationTracker.empty());
        randomProjection.currentEmbedding(-1).copyTo(randomVectors, 3);
        randomProjection.propagateEmbeddings();
        HugeObjectArray<float[]> embeddings = randomProjection.embeddings();

        boolean isEqual = true;
        for (int i = 0; i < 128; i++) {
            isEqual &= embeddings.get(0)[i] == (randomVectors.get(1)[i] + randomVectors.get(2)[i]) / 2.0f;
        }
        assertTrue(isEqual);
    }

    @Test
    void shouldAverageNeighborsWeighted() {
        GraphLoader graphLoader = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Node1")
            .addNodeLabel("Node2")
            .addRelationshipProperty("weight", "weight", DefaultValue.of(1.0), Aggregation.NONE)
            .build();

        Graph graph = graphLoader.graph();

        var weightedConfig = ImmutableRandomProjectionBaseConfig
            .builder()
            .from(DEFAULT_CONFIG)
            .relationshipWeightProperty("weight")
            .embeddingSize(2)
            .build();

        RandomProjection randomProjection = new RandomProjection(
            graph,
            weightedConfig,
            progressLogger,
            AllocationTracker.empty()
        );

        randomProjection.initRandomVectors();
        HugeObjectArray<float[]> randomVectors = HugeObjectArray.newArray(float[].class, 3, AllocationTracker.empty());
        randomProjection.currentEmbedding(-1).copyTo(randomVectors, 3);
        randomProjection.propagateEmbeddings();
        HugeObjectArray<float[]> embeddings = randomProjection.embeddings();

        boolean isEqual = true;
        for (int i = 0; i < 2; i++) {
            isEqual &= embeddings.get(0)[i] == (2.0 * randomVectors.get(1)[i] +  1 * randomVectors.get(2)[i]) / 2.0f;
        }

        assertTrue(isEqual);
    }

    @Test
    void shouldDistributeValuesCorrectly() {
        GraphLoader graphLoader = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Node1")
            .addNodeLabel("Node2")
            .build();

        Graph graph = graphLoader.graph();

        RandomProjection randomProjection = new RandomProjection(
            graph,
            ImmutableRandomProjectionBaseConfig.builder()
                .embeddingSize(512)
                .maxIterations(1)
                .build(),
            progressLogger,
            AllocationTracker.empty()
        );

        randomProjection.initRandomVectors();
        double p = 1D / 6D;
        int maxNumPositive = (int) ((p + 5D * Math.sqrt((p * (1 - p)) / 512D)) * 512D); // 1:30.000.000 chance of failing :P
        int minNumPositive = (int) ((p - 5D * Math.sqrt((p * (1 - p)) / 512D)) * 512D);
        HugeObjectArray<float[]> randomVectors = randomProjection.currentEmbedding(-1);
        for (int i = 0; i < graph.nodeCount(); i++) {
            float[] embedding = randomVectors.get(i);
            int numZeros = 0;
            int numPositive = 0;
            for (int j = 0; j < 512; j++) {
                double embeddingValue = embedding[j];
                if (embeddingValue == 0) {
                    numZeros++;
                } else if (embeddingValue > 0) {
                    numPositive++;
                }
            }

            int numNegative = 512 - numZeros - numPositive;
            assertTrue(numPositive >= minNumPositive && numPositive <= maxNumPositive);
            assertTrue(numNegative >= minNumPositive && numNegative <= maxNumPositive);
        }
    }

    @Test
    void shouldYieldEmptyEmbeddingForIsolatedNodes() {
        GraphLoader graphLoader = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Isolated")
            .build();

        Graph graph = graphLoader.graph();

        RandomProjection randomProjection = new RandomProjection(
            graph,
            ImmutableRandomProjectionBaseConfig.builder()
                .embeddingSize(64)
                .maxIterations(4)
                .build(),
            progressLogger,
            AllocationTracker.empty()
        );

        RandomProjection computeResult = randomProjection.compute();
        HugeObjectArray<float[]> embeddings = computeResult.embeddings();
        for (int i = 0; i < embeddings.size(); i++) {
            float[] embedding = embeddings.get(i);
            for (double embeddingValue : embedding) {
                assertEquals(0.0f, embeddingValue);
            }
        }
    }

    @Test
    void testMemoryEstimationWithoutIterationWeights() {
        var config = ImmutableRandomProjectionBaseConfig
            .builder()
            .maxIterations(2)
            .embeddingSize(128)
            .build();

        var dimensions = ImmutableGraphDimensions.builder().nodeCount(100).build();

        var estimate = RandomProjection.memoryEstimation(config).estimate(dimensions, 1).memoryUsage();
        assertEquals(estimate.min, estimate.max);
        assertEquals(209744, estimate.min);
    }

    @Test
    void testMemoryEstimationWithIterationWeights() {
        var config = ImmutableRandomProjectionBaseConfig
            .builder()
            .maxIterations(2)
            .embeddingSize(128)
            .iterationWeights(List.of(1.0D, 2.0D))
            .build();

        var dimensions = ImmutableGraphDimensions.builder().nodeCount(100).build();

        var estimate = RandomProjection.memoryEstimation(config).estimate(dimensions, 1).memoryUsage();
        assertEquals(estimate.min, estimate.max);
        assertEquals(158544, estimate.min);
    }
}
