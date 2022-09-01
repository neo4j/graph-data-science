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
package org.neo4j.gds.ml.pipeline.node.regression.configure;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.pipeline.AutoTuningConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig.DEFAULT_CONFIG;

class NodeRegressionPipelineAddStepProcsTest extends NodeRegressionPipelineBaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(NodeRegressionPipelineAddStepProcs.class);

        var pipeline = new NodeRegressionTrainingPipeline();

        PipelineCatalog.set(getUsername(), "myPipeline", pipeline);
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void shouldAddNodePropertyStep() {
        var pipeline = PipelineCatalog.get(getUsername(), "myPipeline");

        assertThat(pipeline.nodePropertySteps()).isEmpty();

        Map<String, Object> expectedConfig = Map.of("mutateProperty", "pr");
        String expectedTaskName = "gds.pageRank.mutate";
        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})",
            List.of(Map.of(
                    "name", "myPipeline",
                    "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                    "splitConfig", DEFAULT_CONFIG.toMap(),
                    "nodePropertySteps", List.of(Map.of(
                        "name", expectedTaskName,
                        "config", expectedConfig
                    )),
                    "featureProperties", List.of(),
                    "parameterSpace", DEFAULT_PARAMETERSPACE
                )
            )
        );

        assertThat(pipeline.nodePropertySteps())
            .hasSize(1)
            .contains(NodePropertyStepFactory.createNodePropertyStep(expectedTaskName, expectedConfig));
    }

    @Test
    void shouldSelectFeatures() {
        var pipeline = PipelineCatalog.getTyped(getUsername(), "myPipeline", NodeRegressionTrainingPipeline.class);

        runQuery("CALL gds.alpha.pipeline.nodeRegression.selectFeatures('myPipeline', 'test')");

        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.selectFeatures('myPipeline', ['pr', 'pr2'])",
            List.of(Map.of(
                    "name", "myPipeline",
                    "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                    "splitConfig", DEFAULT_CONFIG.toMap(),
                    "nodePropertySteps", List.of(),
                    "featureProperties", List.of("test", "pr", "pr2"),
                    "parameterSpace", DEFAULT_PARAMETERSPACE
                )
            )
        );

        assertThat(pipeline.featureSteps()).containsExactlyInAnyOrder(
            NodeFeatureStep.of("test"),
            NodeFeatureStep.of("pr"),
            NodeFeatureStep.of("pr2")
        );
    }

    @Test
    void failOnIncompleteNodePropertyStepConfig() {
        assertError("CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('myPipeline', 'fastRP', {})", Map.of(), List.of(
            "Multiple errors in configuration arguments:",
            "No value specified for the mandatory configuration parameter `embeddingDimension`",
            "No value specified for the mandatory configuration parameter `mutateProperty`"
        ));
    }

    @Test
    void failOnDuplicateMutateProperty() {
        runQuery("CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'test'})");
        assertError(
            "CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'test'})",
            "The value of `mutateProperty` is expected to be unique, " +
            "but test was already specified in the gds.pageRank.mutate procedure."
        );
    }

    @Test
    void failOnUnexpectedConfigKeysInNodePropertyStepConfig() {
        assertError(
            "CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('myPipeline', 'pageRank', {" +
            "mutateProperty: 'test', destroyEverything: true" +
            "})",
            "Unexpected configuration key: destroyEverything"
        );
    }

    @Test
    void shouldAddNodeAndSelectFeatureProperties() {
        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.selectFeatures('myPipeline', 'pr')");
        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.selectFeatures('myPipeline', 'pr2')",
            List.of(Map.of(
                "name", "myPipeline",
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                "splitConfig", DEFAULT_CONFIG.toMap(),
                "nodePropertySteps", List.of(Map.of(
                    "name", "gds.pageRank.mutate",
                    "config", Map.of("mutateProperty", "pr")
                )),
                "featureProperties", List.of("pr", "pr2"),
                "parameterSpace", DEFAULT_PARAMETERSPACE
            ))
        );
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForNodePropertyStep() {
        assertError(
            "CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('not a pipe', 'pageRank', {mutateProperty: 'pr'})",
            "Pipeline with name `not a pipe` does not exist for user ``."
        );
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForFeatureStep() {
        assertError(
            "CALL gds.alpha.pipeline.nodeRegression.selectFeatures('not a pipe', 'test')",
            "Pipeline with name `not a pipe` does not exist for user ``."
        );
    }

    @Test
    void shouldThrowInvalidNodePropertyStepName() {
        assertError(
            "CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('myPipeline', 'juggleSpoons', {mutateProperty: 'pr'})",
            "Could not find a procedure called gds.jugglespoons.mutate"
        );
    }

    @Test
    void failOnConfiguringReservedConfigFields() {
        assertError(
            "CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('myPipeline', 'pageRank', {" +
            "mutateProperty: 'pr'," +
            " nodeLabels: 'LABEL'" +
            "})",
            "Cannot configure ['nodeLabels', 'relationshipTypes'] for an individual node property step."
        );
    }

    @Test
    void shouldThrowIfAddingNodePropertyToANonPipeline() {
        PipelineCatalog.set(getUsername(), "testPipe", new LinkPredictionTrainingPipeline());

        assertError(
            "CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('testPipe', 'pageRank', {mutateProperty: 'pr'})",
            "The pipeline `testPipe` is of type `Link prediction training pipeline`, " +
            "but expected type `NodeRegressionTrainingPipeline`."
        );
    }

    @Test
    void shouldThrowIfAddingFeatureToANonPipeline() {
        PipelineCatalog.set(getUsername(), "testPipe", new LinkPredictionTrainingPipeline());

        assertError(
            "CALL gds.alpha.pipeline.nodeRegression.selectFeatures('testPipe', 'pageRank')",
            "The pipeline `testPipe` is of type `Link prediction training pipeline`, " +
            "but expected type `NodeRegressionTrainingPipeline`."
        );
    }
}
