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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.ml.api.TrainingMethod;
import org.neo4j.gds.ml.decisiontree.ClassifierImpurityCriterionType;
import org.neo4j.gds.ml.models.mlp.MLPClassifierTrainConfigImpl;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfigImpl;
import org.neo4j.gds.ml.pipeline.AutoTuningConfig;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineAddStepProcsTest.DEFAULT_SPLIT_CONFIG;

class LinkPredictionPipelineAddTrainerMethodProcsTest extends BaseProcTest {

    private static final String newLine = System.lineSeparator();

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPipelineAddTrainerMethodProcs.class, LinkPredictionPipelineCreateProc.class);

        runQuery("CALL gds.beta.pipeline.linkPrediction.create('myPipeline')");
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void shouldSetParams() {
        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('myPipeline', {minEpochs: 42})",
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", Map.of(
                    TrainingMethod.RandomForestClassification.toString(), List.of(),
                    TrainingMethod.MLPClassification.toString(), List.of(),
                    TrainingMethod.LogisticRegression.toString(), List.of(Map.of(
                        "maxEpochs", 100,
                        "minEpochs", 42,
                        "penalty", 0.0,
                        "patience", 1,
                        "methodName", TrainingMethod.LogisticRegression.toString(),
                        "batchSize", 100,
                        "learningRate", 0.001,
                        "tolerance", 0.001,
                        "focusWeight", 0.0,
                        "classWeights", List.of()
                    ))
                )
            ))
        );
    }

    @Test
    void shouldAddMultipleModels() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('myPipeline', {minEpochs: 42, focusWeight: 2, classWeights: [0.7, 0.3]})");

        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('myPipeline', {minEpochs: 4})",
            List.of(Map.of("name",
                "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", Map.of(
                    TrainingMethod.RandomForestClassification.toString(), List.of(),
                    TrainingMethod.MLPClassification.toString(), List.of(),
                    TrainingMethod.LogisticRegression.toString(), List.of(
                        Map.of(
                            "maxEpochs", 100,
                            "minEpochs", 42,
                            "penalty", 0.0,
                            "patience", 1,
                            "methodName", TrainingMethod.LogisticRegression.toString(),
                            "batchSize", 100,
                            "learningRate", 0.001,
                            "tolerance", 0.001,
                            "focusWeight", 2,
                            "classWeights", List.of(0.7, 0.3)
                        ),
                        Map.of(
                            "maxEpochs", 100,
                            "minEpochs", 4,
                            "penalty", 0.0,
                            "patience", 1,
                            "methodName", TrainingMethod.LogisticRegression.toString(),
                            "batchSize", 100,
                            "learningRate", 0.001,
                            "tolerance", 0.001,
                            "focusWeight", 0.0,
                            "classWeights", List.of()
                        )
                    )
                )
            ))
        );
    }

    @Test
    void addRandomForest() {
        assertCypherResult(
            "CALL gds.pipeline.linkPrediction.addRandomForest('myPipeline', {criterion: 'ENTROPY', maxDepth: 42, maxFeaturesRatio: 0.5, numberOfDecisionTrees: 10, minSplitSize: 2})",
            List.of(Map.of("name",
                "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", Map.of(
                    TrainingMethod.RandomForestClassification.toString(),
                    List.of(RandomForestClassifierTrainerConfigImpl.builder()
                        .criterion(ClassifierImpurityCriterionType.ENTROPY)
                        .maxDepth(42)
                        .maxFeaturesRatio(0.5)
                        .numberOfDecisionTrees(10)
                        .minSplitSize(2)
                        .build()
                        .toMapWithTrainerMethod()),
                    TrainingMethod.LogisticRegression.name(), List.of(),
                    TrainingMethod.MLPClassification.toString(), List.of()
                )
            ))
        );
    }

    @Test
    void addMLP() {
        assertCypherResult(
                "CALL gds.alpha.pipeline.linkPrediction.addMLP('myPipeline', {hiddenLayerSizes: [16,4], penalty: 0.1, batchSize: 10, minEpochs: 2, patience: 2, maxEpochs: 2, tolerance: 1e-4, learningRate: 0.1})",
                List.of(Map.of("name",
                        "myPipeline",
                        "splitConfig", DEFAULT_SPLIT_CONFIG,
                        "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                        "nodePropertySteps", List.of(),
                        "featureSteps", List.of(),
                        "parameterSpace", Map.of(
                                TrainingMethod.MLPClassification.toString(),
                                List.of(MLPClassifierTrainConfigImpl.builder()
                                        .hiddenLayerSizes(List.of(16,4))
                                        .penalty(0.1)
                                        .batchSize(10)
                                        .minEpochs(2)
                                        .patience(2)
                                        .maxEpochs(2)
                                        .tolerance(0.0001)
                                        .learningRate(0.1)
                                        .build()
                                        .toMapWithTrainerMethod()),
                                TrainingMethod.LogisticRegression.name(), List.of(),
                                TrainingMethod.RandomForestClassification.toString(), List.of()
                                )
                ))
        );
    }

    @Test
    void failOnInvalidParameterValues() {
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('myPipeline', {minEpochs: 0.5, batchSize: 0.51})",
            "Multiple errors in configuration arguments:" + newLine +
            "\t\t\t\tThe value of `batchSize` must be of type `Integer` but was `Double`." + newLine +
            "\t\t\t\tThe value of `minEpochs` must be of type `Integer` but was `Double`."
        );
    }

    @Test
    void failOnInvalidKeys() {
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('myPipeline', {invalidKey: 42, penaltE: -0.51})",
            "Unexpected configuration keys: invalidKey, penaltE (Did you mean one of [penalty, patience]?)"
        );
    }

}
