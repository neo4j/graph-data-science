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
package org.neo4j.graphalgo.embeddings.graphsage;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.ddl4j.Tensor;
import org.neo4j.graphalgo.ddl4j.functions.Weights;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GraphSageModelTest {

    @Test
    void smokeTestEmbedding() {
        Weights layer1Weights = new Weights(Tensor.matrix(
            new double[]{
                0.1, 0.1, 0.1,
                0.4, 0.3, 0.9,
                0.01, 0.6, 0.5
            },
            3,
            3
        ));

        Weights layer2Weights = new Weights(Tensor.matrix(
            new double[]{
                0.35, 0.1, 0.3,
                0.25, 0.4, 0.9,
                0.15, 0.3, 0.5
            },
            3,
            3
        ));

        Graph graph = TestGraph.Builder.fromGdl(
            "(a)-[]->(b)-[]->(c), " +
            "(a)-[]->(d), " +
            "(a)-[]->(e), " +
            "(a)-[]->(f), " +
            "(h), " +
            "(g)-[]->(i), " +
            "(i)-[]->(a), " +
            "(i)-[]->(j), " +
            "(j)-[]->(b)");

        Layer layer1 = new MeanAggregatingLayer(graph, layer1Weights, 3);
        Layer layer2 = new MeanAggregatingLayer(graph, layer2Weights, 2);

        GraphSageModel model = new GraphSageModel(layer1, layer2);
        HugeObjectArray<double[]> features = HugeObjectArray.of(
            new double[]{1, 2, 12},
            new double[]{3, 4, 14},
            new double[]{5, 6, 16},
            new double[]{7, 8, 18},
            new double[]{9, 10, 11},
            new double[]{11, 12, 12},
            new double[]{13, 14, 14},
            new double[]{15, 16, 11},
            new double[]{17, 18, 8},
            new double[]{19, 20, 2}
        );

        HugeObjectArray<double[]> result = model.makeEmbeddings(graph, features);

        assertNotNull(result);
        assertEquals(10, result.size());
        for(int i = 0; i < result.size(); i++) {
            // Equals to the number of rows in the weights,
            // may be the weights should be Square Matrix with dimensions (nodeFeatures.size x nodeFeatures.size)
            assertEquals(3, result.get(i).length);
        }
    }
}