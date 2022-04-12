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
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainerConfigImpl;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCompanion.DEFAULT_SPLIT_CONFIG;
import static org.neo4j.gds.ml.pipeline.AutoTuningConfig.MAX_TRIALS;


class NodeClassificationPipelineAddTrainerMethodProcsTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(NodeClassificationPipelineAddTrainerMethodProcs.class, NodeClassificationPipelineCreateProc.class);

        runQuery("CALL gds.beta.pipeline.nodeClassification.create('myPipeline')");
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void shouldSetLRParams() {
        assertCypherResult(
            "CALL gds.beta.pipeline.nodeClassification.addLogisticRegression('myPipeline', {minEpochs: 42})",
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", Map.of("maxTrials", MAX_TRIALS),
                "nodePropertySteps", List.of(),
                "featureProperties", List.of(),
                "parameterSpace", Map.of(
                    TrainingMethod.RandomForest.name(), List.of(),
                    TrainingMethod.LogisticRegression.name(), List.of(Map.of(
                            "maxEpochs", 100,
                            "minEpochs", 42,
                            "methodName", TrainingMethod.LogisticRegression.name(),
                            "penalty", 0.0,
                            "patience", 1,
                            "batchSize", 100,
                        "learningRate", 0.001,
                            "tolerance", 0.001
                        ))
                )
            ))
        );
    }

    @Test
    void shouldSetRFParams() {
        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeClassification.addRandomForest('myPipeline', {maxDepth: 42, maxFeaturesRatio: 0.5, numberOfDecisionTrees: 10, minSplitSize: 2})",
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", Map.of("maxTrials", MAX_TRIALS),
                "nodePropertySteps", List.of(),
                "featureProperties", List.of(),
                "parameterSpace", Map.of(
                    TrainingMethod.RandomForest.name(),
                    List.of(RandomForestTrainerConfigImpl
                        .builder()
                        .maxDepth(42)
                        .maxFeaturesRatio(0.5)
                        .numberOfDecisionTrees(10)
                        .minSplitSize(2)
                        .build()
                        .toMap()),
                    TrainingMethod.LogisticRegression.name(), List.of()
                )
            ))
        );
    }

    @Test
    void shouldKeepBothConfigs() {
        runQuery("CALL gds.beta.pipeline.nodeClassification.addLogisticRegression('myPipeline', {minEpochs: 42})");

        assertCypherResult(
            "CALL gds.beta.pipeline.nodeClassification.addLogisticRegression('myPipeline', {minEpochs: 4})",
            List.of(Map.of("name",
                "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", Map.of("maxTrials", MAX_TRIALS),
                "nodePropertySteps", List.of(),
                "featureProperties", List.of(),
                "parameterSpace", Map.of(
                    TrainingMethod.RandomForest.name(), List.of(),
                    TrainingMethod.LogisticRegression.name(), List.of(
                        Map.of(
                            "maxEpochs", 100,
                            "minEpochs", 42,
                            "penalty", 0.0,
                            "patience", 1,
                            "methodName", TrainingMethod.LogisticRegression.name(),
                            "batchSize", 100,
                            "learningRate", 0.001,
                            "tolerance", 0.001
                        ),
                        Map.of(
                            "maxEpochs", 100,
                            "minEpochs", 4,
                            "penalty", 0.0,
                            "patience", 1,
                            "methodName", TrainingMethod.LogisticRegression.name(),
                            "batchSize", 100,
                            "learningRate", 0.001,
                            "tolerance", 0.001
                        )
                    ))
            ))
        );
    }

    @Test
    void failOnInvalidParameterValues() {
        assertError(
            "CALL gds.beta.pipeline.nodeClassification.addLogisticRegression('myPipeline', {minEpochs: 0.5, batchSize: 0.51})",
            "Multiple errors in configuration arguments:\n" +
            "\t\t\t\tThe value of `batchSize` must be of type `Integer` but was `Double`.\n" +
            "\t\t\t\tThe value of `minEpochs` must be of type `Integer` but was `Double`."
        );
    }

    @Test
    void failOnInvalidKeys() {
        assertError(
            "CALL gds.beta.pipeline.nodeClassification.addLogisticRegression('myPipeline', {invalidKey: 42, penaltE: -0.51})",
            "Unexpected configuration keys: invalidKey, penaltE (Did you mean one of [penalty, patience]?)"
        );
    }

}
