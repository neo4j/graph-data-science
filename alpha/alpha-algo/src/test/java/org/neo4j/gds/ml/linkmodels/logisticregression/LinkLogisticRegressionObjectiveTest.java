/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.linkmodels.logisticregression;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.ml.Batch;
import org.neo4j.gds.ml.LazyBatch;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;

import static org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid.sigmoid;

@GdlExtension
class LinkLogisticRegressionObjectiveTest {

    @GdlGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (n1:N {a: 2.0, b: 1.2})" +
        ", (n2:N {a: 1.3, b: 0.5})" +
        ", (n3:N {a: 0.0, b: 2.8})" +
        ", (n4:N {a: 1.0, b: 0.9})" +
        ", (n1)-[:T {label: 1.0}]->(n2)" +
        ", (n3)-[:T {label: 1.0}]->(n4)" +
        ", (n1)-[:T {label: 0.0}]->(n3)" +
        ", (n2)-[:T {label: 0.0}]->(n4)";

    @Inject
    private Graph graph;

    private Batch allNodesBatch;

    @BeforeEach
    void setup() {
        allNodesBatch = new LazyBatch(0, (int) graph.nodeCount(), graph.nodeCount());
    }

    @Test
    void shouldProduceCorrectLoss() {
        var objective = new LinkLogisticRegressionObjective(List.of("a", "b"), LinkFeatureCombiner.L2, graph);
        var loss = objective.loss(allNodesBatch, graph.relationshipCount());
        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();
        Assertions.assertThat(lossValue).isEqualTo(-Math.log(0.5));
    }

    @Test
    void shouldProduceCorrectLossWithNonZeroWeights() {
        var objective = new LinkLogisticRegressionObjective(List.of("a", "b"), LinkFeatureCombiner.L2, graph);
        // we proceed to "injecting" non-zero values for the weights of the model
        var weights = objective.weights();
        var backingArray = weights.get(0).data().data();
        backingArray[0] = 0.2;
        backingArray[1] = -0.3;
        backingArray[2] = 0.5;
        var loss = objective.loss(allNodesBatch, graph.relationshipCount());
        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        var pred = new double[]{
            sigmoid(0.2 * 0.49 - 0.3 * 0.49 + 0.5),
            sigmoid(0.2 * 4 - 0.3 * 2.56 + 0.5),
            sigmoid(0.2 * 0.09 - 0.3 * 0.16 + 0.5),
            sigmoid(0.2 * 1.0 - 0.3 * 3.61 + 0.5)
        };
        var expectedTotalLoss = -Math.log(pred[0]) - Math.log(1 - pred[1]) - Math.log(1 - pred[2]) - Math.log(pred[3]);
        Assertions.assertThat(lossValue).isCloseTo(expectedTotalLoss/4, Offset.offset(1e-7));
    }
}
