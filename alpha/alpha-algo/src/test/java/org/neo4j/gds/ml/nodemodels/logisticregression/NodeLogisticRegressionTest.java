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
package org.neo4j.gds.ml.nodemodels.logisticregression;

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
class NodeLogisticRegressionTest {

    @GdlGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (n1:N {a: 2.0, b: 1.2, t: 1.0})" +
        ", (n2:N {a: 1.3, b: 0.5, t: 0.0})" +
        ", (n3:N {a: 0.0, b: 2.8, t: 0.0})" +
        ", (n4:N {a: 1.0, b: 0.9, t: 1.0})";

    @Inject
    private Graph graph;

    private Batch allNodesBatch;

    @BeforeEach
    void setup() {
        allNodesBatch = new LazyBatch(0, (int) graph.nodeCount(), graph.nodeCount());
    }

    @Test
    void shouldProduceCorrectLoss() {
        var model = new NodeLogisticRegression(List.of("a", "b"), "t", graph);
        var loss = model.loss(allNodesBatch);
        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();
        // all predictions are sigmoid(0) = 0.5
        // the loss is crossentropy -log(pred) for positive examples and -log(1-pred) for negative examples
        // both become -log(0.5)
        Assertions.assertThat(lossValue).isEqualTo(-Math.log(0.5));
    }

    @Test
    void shouldProduceCorrectLossWithNonZeroWeights() {
        var model = new NodeLogisticRegression(List.of("a", "b"), "t", graph);
        // we proceed to "injecting" non-zero values for the weights of the model
        var weights = model.weights();
        var backingArray = weights.get(0).data().data();
        backingArray[0] = 0.2;
        backingArray[1] = -0.3;
        backingArray[2] = 0.5;
        var loss = model.loss(allNodesBatch);
        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        var pred = new double[]{
            sigmoid(0.2 * 2.0 - 0.3 * 1.2 + 0.5),
            sigmoid(0.2 * 1.3 - 0.3 * 0.5 + 0.5),
            sigmoid(0.2 * 0.0 - 0.3 * 2.8 + 0.5),
            sigmoid(0.2 * 1.0 - 0.3 * 0.9 + 0.5)
        };
        var expectedTotalLoss = -Math.log(pred[0]) - Math.log(pred[3]) - Math.log(1 - pred[1]) - Math.log(1 - pred[2]);
        Assertions.assertThat(lossValue).isCloseTo(expectedTotalLoss/4, Offset.offset(1e-8));
    }
}
