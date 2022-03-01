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
package org.neo4j.gds.ml.nodemodels;

import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.MessageCollectingProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.Features;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionTrainer;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrain;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfigImpl;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@GdlExtension
class NCTrainerCompareTest {

    @GdlGraph
    private static final String GDL = generateGraph();

    @Inject
    private Graph graph;

    private static String generateGraph() {
        return "({a: 1, b: [21.0, 3.0], c: 0.7, class: 0})," +
               "({a: 1, b: [20.0, 3.0], c: -0.7, class: 1})," +
               "({a: 1, b: [21.0, 3.0], c: -0.7, class: 2})," +
               "({a: 1, b: [22.0, 3.0], c: -0.7, class: -90})," +
               "({a: 1, b: [21.0, 3.0], c: -0.7, class: 202})," +
               "({a: 1, b: [21.0, 3.0], c: 10.7, class: 0})," +
               "({a: 1, b: [21.0, 3.0], c: 10.7, class: 0})," +
               "({a: 1, b: [21.0, 3.0], c: 10.7, class: 0})," +
               "({a: 1, b: [21.0, 3.0], c: 10.7, class: -90})," +
               "({a: 1, b: [21.0, 3.0], c: 10.7, class: 2})" +
               "";
    }

    private LogisticRegressionTrainConfig convertConfig(NodeLogisticRegressionTrainConfig nlrConfig) {
        return LogisticRegressionTrainConfigImpl.builder()
            .batchSize(nlrConfig.batchSize())
            .maxEpochs(nlrConfig.maxEpochs())
            .minEpochs(nlrConfig.minEpochs())
            .patience(nlrConfig.patience())
            .penalty(nlrConfig.penalty())
            .tolerance(nlrConfig.tolerance())
            .useBiasFeature(true).build();
    }

    @ParameterizedTest
    @ValueSource(doubles = {0.0, 0.01, 0.5, 1.0})
    void shouldBeTheSame(double penalty) {
        HugeLongArray trainSet = HugeLongArray.of(1, 2, 3);
        var oldConfig = NodeLogisticRegressionTrainConfigImpl
            .builder()
            .featureProperties(List.of("a", "b", "c"))
            .targetProperty("class")
            .batchSize(2)
            .maxEpochs(10)
            .penalty(penalty)
            .build();

        var progressTracker = new MessageCollectingProgressTracker();
        var oldTrainer = new NodeLogisticRegressionTrain(
            graph,
            trainSet,
            oldConfig,
            progressTracker,
            TerminationFlag.RUNNING_TRUE,
            1
        );
        var oldData = oldTrainer.compute();
//        progressTracker.printAndClear();

        var newTargets = HugeLongArray.newArray(graph.nodeCount());
        for (long nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            newTargets.set(nodeId, graph.nodeProperties("class").longValue(nodeId));
        }
        var newClassIdMap = NodeClassificationTrain.makeClassIdMap(newTargets);
        var newFeatures = Features.extractLazyFeatures(graph, oldConfig.featureProperties());

        var newTrainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(trainSet),
            1,
            convertConfig(oldConfig),
            newClassIdMap,
            false,
            TerminationFlag.RUNNING_TRUE,
            progressTracker
        );
        LogisticRegressionClassifier classifier = newTrainer.train(newFeatures, newTargets);
//        progressTracker.printAndClear();

        assertThat(classifier.convertToPredictor(List.of("class")).modelData().weights().data().data()).containsExactly(
            oldData.weights().data().data(),
            Offset.offset(1e-9)
        );
    }
}
