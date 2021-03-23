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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.ImmutableMultiClassNLRTrainConfig;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions.ROWS_INDEX;

@GdlExtension
class NodeLogisticRegressionTrainTest {

    @GdlGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (n1:N {a: 2.0, b: 1.2, t: 1.0})" +
        ", (n2:N {a: 1.3, b: 0.5, t: 0.0})" +
        ", (n3:N {a: 0.0, b: 2.8, t: 0.0})" +
        ", (n4:N {a: 1.0, b: 0.9, t: 1.0})";

    @Inject
    private Graph graph;

    @Test
    void shouldComputeWithDefaultAdamOptimizerAndStreakStopper() {
        var config =
            ImmutableMultiClassNLRTrainConfig.builder()
                .featureProperties(List.of("a", "b"))
                .targetProperty("t")
                .maxIterations(100000)
                .tolerance(1e-4)
                .penalty(0.0)
                .concurrency(1)
                .build();
        var linearRegression = new NodeLogisticRegressionTrain(graph, config, ProgressLogger.NULL_LOGGER);

        var result = linearRegression.compute();

        assertThat(result).isNotNull();

        var trainedWeights = result.weights();
        assertThat(trainedWeights.dimension(ROWS_INDEX)).isEqualTo(1);
        assertThat(trainedWeights.dimension(COLUMNS_INDEX)).isEqualTo(3);

        assertThat(trainedWeights.data().data()).containsExactly(
            new double[]{0.7022941899846009, -0.4539546939516586, -0.186729778863158},
            Offset.offset(1e-8)
        );
    }
}
