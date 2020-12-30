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
package org.neo4j.gds.ml.nodemodels.multiclasslogisticregression;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.ml.LazyBatch;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;

import static java.lang.Math.log;

@GdlExtension
class MultiClassNodeLogisticRegressionTest {

    private static final int NUMBER_OF_CLASSES = 3;

    @GdlGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (n1:N {a: 2.0, b: 1.2, t: 1.0})" +
        ", (n2:N {a: 1.3, b: 0.5, t: 0.0})" +
        ", (n3:N {a: 0.0, b: 2.8, t: 2.0})" +
        ", (n4:N {a: 1.0, b: 0.9, t: 1.0})";

    @Inject
    private Graph graph;

    @Test
    void shouldProduceCorrectLoss() {
        var allNodesBatch = new LazyBatch(0, (int) graph.nodeCount(), graph.nodeCount());
        var model = new MultiClassNodeLogisticRegression(List.of("a", "b"), "t", graph);
        var loss = model.loss(allNodesBatch);
        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        // weights are zero => each class has equal probability to be correct.
        Assertions.assertThat(lossValue).isEqualTo(-log(1.0 / NUMBER_OF_CLASSES));
    }

}
