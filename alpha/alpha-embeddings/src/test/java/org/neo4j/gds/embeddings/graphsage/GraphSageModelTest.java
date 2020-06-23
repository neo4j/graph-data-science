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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.embeddings.graphsage.ddl4j.FiniteDifferenceTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.LayerInitialisationFactory.ActivationFunction;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.embeddings.graphsage.LayerInitialisationFactory.ActivationFunctions.SIGMOID;

@GdlExtension
class GraphSageModelTest implements FiniteDifferenceTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String GRAPH =
        "(a)-[]->(b)-[]->(c), " +
        "(a)-[]->(d), " +
        "(a)-[]->(e), " +
        "(a)-[]->(f), " +
        "(g)-[]->(i), " +
        "(i)-[]->(a), " +
        "(i)-[]->(j), " +
        "(j)-[]->(b)";


    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    private static final int CONCURRENCY = 4;
    private static final ActivationFunction ACTIVATION_FUNCTION = SIGMOID;
    private Layer mockLayer1;
    private Layer mockLayer2;
    private HugeObjectArray<double[]> features;
    private static final Weights LAYER_1_WEIGHTS = new Weights(Tensor.matrix(
        new double[]{
            0.1, 0.1, 0.1,
            0.4, 0.3, 0.9,
            0.01, 0.6, 0.5
        },
        3,
        3
    ));
    private static final Weights LAYER_2_WEIGHTS = new Weights(Tensor.matrix(
        new double[]{
            0.35, 0.1, 0.3,
            0.25, 0.4, 0.9,
            0.15, 0.3, 0.5
        },
        3,
        3
    ));

    @BeforeEach
    void setup() {

        Layer layer1 = new MeanAggregatingLayer(LAYER_1_WEIGHTS, 3, ACTIVATION_FUNCTION.activationFunction());
        Layer layer2 = new MeanAggregatingLayer(LAYER_2_WEIGHTS, 2, ACTIVATION_FUNCTION.activationFunction());
        mockLayer1 = spy(layer1);
        mockLayer2 = spy(layer2);
        when(mockLayer1.randomState()).thenReturn(0L);
        when(mockLayer2.randomState()).thenReturn(0L);

        features = HugeObjectArray.of(
            new double[]{1, 2, 12},
            new double[]{3, 4, 14},
            new double[]{5, 6, 16},
            new double[]{7, 8, 18},
            new double[]{9, 10, 11},
            new double[]{11, 12, 12},
            new double[]{13, 14, 14},
            new double[]{15, 16, 11},
            new double[]{17, 18, 8}
        );

    }

    @Override
    public double epsilon() {
        return 1e-8;
    }

    @Test
    void testLoss() {
        GraphSageModel model = new GraphSageModel(CONCURRENCY, 3, List.of(mockLayer1, mockLayer2), new TestLog());
        List<Long> batch = List.of(
            idFunction.of("a"),
            idFunction.of("b")
        );

        finiteDifferenceShouldApproximateGradient(
            List.of(LAYER_1_WEIGHTS, LAYER_2_WEIGHTS),
            model.lossFunction(batch, graph, features)
        );

    }

    @Test
    void smokeTestEmbedding() {
        smokeTestEmbeddingHelper(
            new GraphSageModel(CONCURRENCY, 3, List.of(mockLayer1, mockLayer2), new TestLog())
        );
    }

    @Test
    void smokeTestEmbeddingSingleBatch() {
        smokeTestEmbeddingHelper(
            new GraphSageModel(CONCURRENCY, (int) graph.nodeCount(), List.of(mockLayer1, mockLayer2), new TestLog())
        );
    }

    @Test
    void compareSingleAndMiniBatch() {
        Log log = FormattedLog.withLogLevel(Level.DEBUG).toOutputStream(System.out);

        GraphSageModel miniBatchModel = new GraphSageModel(CONCURRENCY, 5, List.of(mockLayer1, mockLayer2), log);
        GraphSageModel singleBatchModel =
            new GraphSageModel(CONCURRENCY, (int) graph.nodeCount(), List.of(mockLayer1, mockLayer2), log);
        HugeObjectArray<double[]> singleBatchResult = smokeTestEmbeddingHelper(miniBatchModel);
        HugeObjectArray<double[]> miniBatchResult = smokeTestEmbeddingHelper(singleBatchModel);
        for (int i = 0; i < 9; i++) {
            assertArrayEquals(singleBatchResult.get(i), miniBatchResult.get(i));
        }
    }

    HugeObjectArray<double[]> smokeTestEmbeddingHelper(GraphSageModel model) {
        HugeObjectArray<double[]> result = model.makeEmbeddings(graph, features);

        assertNotNull(result);
        assertEquals(9, result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(3, result.get(i).length);
        }

        return result;
    }
}
