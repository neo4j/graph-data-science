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
import org.neo4j.gds.ml.pipeline.AutoTuningConfig;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.DEFAULT_PARAM_SPACE;

class LinkPredictionPipelineAddStepProcsTest extends BaseProcTest {

    static final Map<String, Object> DEFAULT_SPLIT_CONFIG = Map.of(
        "negativeSamplingRatio",
        1.0,
        "testFraction",
        0.1,
        "validationFolds",
        3,
        "trainFraction",
        0.1
    );

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPipelineAddStepProcs.class, LinkPredictionPipelineCreateProc.class);

        runQuery("CALL gds.beta.pipeline.linkPrediction.create('myPipeline')");
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void shouldAddNodePropertyStep() {
        assertCypherResult("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                "nodePropertySteps", List.of(
                    Map.of(
                        "name", "gds.pageRank.mutate",
                        "config", Map.of("mutateProperty", "pr")
                    )),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
    }

    //TODO addNodeProertyWithContextTest

    @Test
    void shouldAddFeatureStep() {
        assertCypherResult("CALL gds.beta.pipeline.linkPrediction.addFeature('myPipeline', 'hadamard', {nodeProperties: ['pr']})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(
                    Map.of(
                        "name", "HADAMARD",
                        "config", Map.of("nodeProperties", List.of("pr"))
                    )),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
    }

    @Test
    void failsWhenAddFeatureStepIsMissingNodeProperties() {
        assertError("CALL gds.beta.pipeline.linkPrediction.addFeature('myPipeline', 'hadamard', {mutateProperty: 'pr'})",
            "No value specified for the mandatory configuration parameter `nodeProperties"
        );
    }

    @Test
    void failOnIncompleteNodePropertyStepConfig() {
        assertThatThrownBy(() -> runQuery(
            "CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'fastRP', {})"
        ))
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .getRootCause()
            .hasMessageContaining("Multiple errors in configuration arguments:")
            .hasMessageContaining("No value specified for the mandatory configuration parameter `embeddingDimension`")
            .hasMessageContaining("No value specified for the mandatory configuration parameter `mutateProperty`");
    }

    @Test
    void failOnDuplicateMutateProperty() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})");
        assertThatThrownBy(() -> runQuery(
            "CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})"
        ))
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "The value of `mutateProperty` is expected to be unique, but pr was already specified in the gds.pageRank.mutate procedure.");
    }

    @Test
    void failOnUnexpectedConfigKeysInNodePropertyStepConfig() {
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr', destroyEverything: true})",
            "Unexpected configuration key: destroyEverything"
        );
    }

    @Test
    void failOnDifferentRelationshipWeightProperties() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr', relationshipWeightProperty: 'foo'})");
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr', relationshipWeightProperty: 'bar'})",
            "Node property steps added to a pipeline may not have different non-null values for `relationshipWeightProperty`. Pipeline already contains tasks `gds.pageRank.mutate` which use the value `foo`."
        );
    }

    @Test
    void shouldAddNodeAndFeatureSteps() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('myPipeline', 'hadamard', {nodeProperties: ['pr']})");
        assertCypherResult("CALL gds.beta.pipeline.linkPrediction.addFeature('myPipeline', 'l2', {nodeProperties: ['pr']})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                "nodePropertySteps", List.of(
                    Map.of(
                        "name", "gds.pageRank.mutate",
                        "config", Map.of("mutateProperty", "pr")
                    )),
                "featureSteps", List.of(
                    Map.of(
                        "name", "HADAMARD",
                        "config", Map.of("nodeProperties", List.of("pr"))
                    ),
                    Map.of(
                        "name", "L2",
                        "config", Map.of("nodeProperties", List.of("pr"))
                    )
                ),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
    }

    @Test
    void shouldAddTwoNodePropertyStep() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})");
        assertCypherResult("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr2'})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig",DEFAULT_SPLIT_CONFIG,
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                "nodePropertySteps", List.of(
                    Map.of(
                        "name", "gds.pageRank.mutate",
                        "config", Map.of("mutateProperty", "pr")
                    ),
                    Map.of(
                        "name", "gds.pageRank.mutate",
                        "config", Map.of("mutateProperty", "pr2")
                    )),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForNodePropertyStep() {
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addNodeProperty('ceci nest pas une pipe', 'pageRank', {mutateProperty: 'pr'})",
            "Pipeline with name `ceci nest pas une pipe` does not exist for user ``."
        );
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForFeatureStep() {
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addFeature('ceci nest pas une pipe', 'hadamard', {nodeProperties: 'pr'})",
            "Pipeline with name `ceci nest pas une pipe` does not exist for user ``."
        );
    }

    @Test
    void shouldThrowInvalidNodePropertyStepName() {
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'juggleSpoons', {mutateProperty: 'pr'})",
            "Could not find a procedure called gds.jugglespoons.mutate"
        );
    }

    @Test
    void failOnConfiguringReservedConfigFields() {
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {nodeLabels: ['LABEL'], mutateProperty: 'pr'})",
            "Cannot configure ['nodeLabels', 'relationshipTypes'] for an individual node property step."
        );
    }

    @Test
    void shouldThrowInvalidFeatureStepName() {
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addFeature('myPipeline', 'juggleSpoons', {nodeProperties: ['pr']})",
            "LinkFeatureStep `juggleSpoons` is not supported. Must be one of: [HADAMARD, COSINE, L2, SAME_CATEGORY]."
        );
    }

    @Test
    void shouldThrowIfAddingNodePropertyToANonLPPipeline() {
        PipelineCatalog.set(getUsername(), "ncPipe", new NodeClassificationTrainingPipeline());

        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addNodeProperty('ncPipe', 'pageRank', {mutateProperty: 'pr'})",
            "The pipeline `ncPipe` is of type `Node classification training pipeline`, but expected type `Link prediction training pipeline`"
        );
    }

    @Test
    void shouldThrowIfAddingFeatureToANonPipeline() {
        PipelineCatalog.set(getUsername(), "ncPipe", new NodeClassificationTrainingPipeline());
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.addFeature('ncPipe', 'pageRank', {mutateProperty: 'pr'})",
            "The pipeline `ncPipe` is of type `Node classification training pipeline`, but expected type `Link prediction training pipeline`"
        );
    }
}
