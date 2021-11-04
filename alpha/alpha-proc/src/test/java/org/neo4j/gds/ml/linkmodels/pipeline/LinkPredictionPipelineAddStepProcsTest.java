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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.InjectModelCatalog;
import org.neo4j.gds.core.ModelCatalogExtension;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.model.catalog.TestTrainConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineConfigureParamsProcTest.DEFAULT_PARAM_CONFIG;

@ModelCatalogExtension
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

    @InjectModelCatalog
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPipelineAddStepProcs.class, LinkPredictionPipelineCreateProc.class);

        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('myPipeline')");
    }

    @Test
    void shouldAddNodePropertyStep() {
        assertCypherResult("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "nodePropertySteps", List.of(
                    Map.of(
                        "name", "gds.pageRank.mutate",
                        "config", Map.of("mutateProperty", "pr")
                    )),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_CONFIG
            ))
        );
    }

    @Test
    void shouldAddFeatureStep() {
        assertCypherResult("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('myPipeline', 'hadamard', {nodeProperties: ['pr']})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(
                    Map.of(
                        "name", "HADAMARD",
                        "config", Map.of("nodeProperties", List.of("pr"))
                    )),
                "parameterSpace", DEFAULT_PARAM_CONFIG
            ))
        );
    }

    @Test
    void failsWhenAddFeatureStepIsMissingNodeProperties() {
        assertError("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('myPipeline', 'hadamard', {mutateProperty: 'pr'})",
            "No value specified for the mandatory configuration parameter `nodeProperties"
        );
    }

    @Test
    void failOnIncompleteNodePropertyStepConfig() {
        assertThatThrownBy(() -> runQuery(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'fastRP', {})"
        ))
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .getRootCause()
            .hasMessageContaining("Multiple errors in configuration arguments:")
            .hasMessageContaining("No value specified for the mandatory configuration parameter `embeddingDimension`")
            .hasMessageContaining("No value specified for the mandatory configuration parameter `mutateProperty`");
    }

    @Test
    void failOnDuplicateMutateProperty() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})");
        assertThatThrownBy(() -> runQuery(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})"
        ))
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "The value of `mutateProperty` is expected to be unique, but pr was already specified in the mutate procedure.");
    }

    @Test
    void failOnUnexpectedConfigKeysInNodePropertyStepConfig() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr', destroyEverything: true})",
            "Unexpected configuration key: destroyEverything"
        );
    }

    @Test
    void failOnDifferentRelationshipWeightProperties() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr', relationshipWeightProperty: 'foo'})");
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr', relationshipWeightProperty: 'bar'})",
            "Node property steps added to a pipeline may not have different non-null values for `relationshipWeightProperty`. Pipeline already contains tasks `pageRank` which use the value `foo`."
        );
    }

    @Test
    void shouldAddNodeAndFeatureSteps() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('myPipeline', 'hadamard', {nodeProperties: ['pr']})");
        assertCypherResult("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('myPipeline', 'l2', {nodeProperties: ['pr']})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
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
                "parameterSpace", DEFAULT_PARAM_CONFIG
            ))
        );
    }

    @Test
    void shouldAddTwoNodePropertyStep() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})");
        assertCypherResult("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr2'})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig",DEFAULT_SPLIT_CONFIG,
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
                "parameterSpace", DEFAULT_PARAM_CONFIG
            ))
        );
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForNodePropertyStep() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('ceci nest pas une pipe', 'pageRank', {mutateProperty: 'pr'})",
            "Model with name `ceci nest pas une pipe` does not exist."
        );
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForFeatureStep() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('ceci nest pas une pipe', 'hadamard', {nodeProperties: 'pr'})",
            "Model with name `ceci nest pas une pipe` does not exist."
        );
    }

    @Test
    void shouldThrowInvalidNodePropertyStepName() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'juggleSpoons', {mutateProperty: 'pr'})",
            "Invalid procedure name `juggleSpoons` for pipelining."
        );
    }

    @Test
    void failOnConfiguringReservedConfigFields() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {nodeLabels: ['LABEL'], mutateProperty: 'pr'})",
            "Cannot configure ['nodeLabels', 'relationshipTypes'] for an individual node property step, but can only be configured at `train` and `predict` mode."
        );
    }

    @Test
    void shouldThrowInvalidFeatureStepName() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('myPipeline', 'juggleSpoons', {mutateProperty: 'pr'})",
            "LinkFeatureStep `juggleSpoons` is not supported. Must be one of: [HADAMARD, COSINE, L2]."
        );
    }

    @Test
    void shouldThrowIfAddingNodePropertyToANonPipeline() {
        var model1 = Model.of(
            getUsername(),
            "testModel1",
            "testAlgo1",
            GraphSchema.empty(),
            "testData",
            TestTrainConfig.of(),
            Map::of
        );

        modelCatalog.set(model1);
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('testModel1', 'pageRank', {mutateProperty: 'pr'})",
            "Steps can only be added to a model of type `Link prediction training pipeline`. But model `testModel1` is of type `testAlgo1`."
        );
    }

    @Test
    void shouldThrowIfAddingFeatureToANonPipeline() {
        var model1 = Model.of(
            getUsername(),
            "testModel1",
            "testAlgo1",
            GraphSchema.empty(),
            "testData",
            TestTrainConfig.of(),
            Map::of
        );

        modelCatalog.set(model1);
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('testModel1', 'pageRank', {mutateProperty: 'pr'})",
            "Steps can only be added to a model of type `Link prediction training pipeline`. But model `testModel1` is of type `testAlgo1`."
        );
    }
}
