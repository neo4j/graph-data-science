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
package org.neo4j.gds.ml.linkmodels;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkFeatureCombiner;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionPredictor;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkPredictionPredictTest {
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static String GDL = "CREATE " +
            "  (n0:N {a: 1.0, b: 0.9})" +
            ", (n1:N {a: 2.0, b: 1.0})" +
            ", (n2:N {a: 3.0, b: 1.5})" +
            ", (n3:N {a: 0.0, b: 2.8})" +
            ", (n4:N {a: 1.0, b: 0.9})" +
            ", (n1)-[:T]->(n2)" +
            ", (n3)-[:T]->(n4)" +
            ", (n1)-[:T]->(n3)" +
            ", (n2)-[:T]->(n4)";

    @Inject
    Graph graph;

    @ParameterizedTest
    @ValueSource(ints = {3, 50})
    void shouldPredictCorrectly(int topN) {
        var numberOfFeatures = 3;
        var numberOfNodeFeatures = 2;
        var modelData = LinkLogisticRegressionData.builder()
            .weights(new Weights<>(new Matrix(new double[]{
                -2.0, -1.0, 3.0,
            }, 1, numberOfFeatures)))
            .linkFeatureCombiner(LinkFeatureCombiner.L2)
            .numberOfNodeFeatures(numberOfNodeFeatures)
            .featureProperties(List.of("a", "b"))
            .build();

        var result = new LinkPredictionPredict(
            new LinkLogisticRegressionPredictor(modelData),
            graph,
            1,
            1,
            topN,
            AllocationTracker.empty(),
            TestProgressLogger.NULL_LOGGER,
            0.0
        ).compute();
        var predictedLinks = result.stream().collect(Collectors.toList());
        assertThat(predictedLinks).hasSize(Math.min(topN, 6));
        var firstLink = predictedLinks.get(0);
        assertThat(firstLink.sourceId()).isEqualTo(0);
        assertThat(firstLink.targetId()).isEqualTo(4);
    }


}
