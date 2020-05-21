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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GraphSageModelTest {

    @Test
    void smokeTestEmbedding() {
        Weights weights = new Weights(Tensor.matrix(
            new double[]{
                0.1, 0.6, 0.3,
                0.4, 0.5, 0.9,
                0.01, 0.7, 0.5,
                0.4, 0.8, 0.5,
            },
            4,
            3
        ));
        Graph graph = TestGraph.Builder.fromGdl(
            "(a)-[]->(b)-[]->(c), (a)-[]->(d) , (a)-[]->(e), (a)-[]->(f), (h), (g)-[]->(i), (i)-[]->(a), (i)-[]->(j), (j)-[]->(b)");
        Layer mean = new MeanAggregatingLayer(graph, weights, 3);

        GraphSageModel model = new GraphSageModel(List.of(mean));
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
            // Equals to the number of rows in the weights
            assertEquals(4, result.get(i).length);
        }
    }
}