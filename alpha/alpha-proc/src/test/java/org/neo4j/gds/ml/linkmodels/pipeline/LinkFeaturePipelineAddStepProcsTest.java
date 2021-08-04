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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.model.catalog.TestTrainConfig;

import java.util.List;
import java.util.Map;

class LinkFeaturePipelineAddStepProcsTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkFeaturePipelineAddStepProcs.class, LinkFeaturePipelineCreateProc.class);

        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('myPipeline')");
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void shouldAddNodePropertyStep() {
        assertCypherResult("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", Map.of(),
                "nodePropertySteps", List.of(
                    Map.of(
                        "name", "pageRank",
                        "config", Map.of("mutateProperty", "pr")
                    )),
                "featureSteps", List.of(),
                "parameterSpace", List.of()
            ))
        );
    }

    @Test
    void shouldAddFeatureStep() {
        assertCypherResult("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('myPipeline', 'hadamard', {nodeProperties: ['pr']})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", Map.of(),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(
                    Map.of(
                        "name", "HADAMARD",
                        "config", Map.of("nodeProperties", List.of("pr"))
                    )),
                "parameterSpace", List.of()
            ))
        );
    }

    @Test
    void failsWhenAddFeatureStepIsMissingNodeProperties() {
        assertError("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('myPipeline', 'hadamard', {mutateProperty: 'pr'})",
            "Configuration for Hadamard link feature is missing `nodeProperties`"
        );
    }

    @Test
    void shouldAddNodeAndFeatureSteps() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('myPipeline', 'hadamard', {nodeProperties: ['pr']})");
        assertCypherResult("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('myPipeline', 'l2', {nodeProperties: ['pr']})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", Map.of(),
                "nodePropertySteps", List.of(
                    Map.of(
                        "name", "pageRank",
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
                "parameterSpace", List.of()
            ))
        );
    }

    @Test
    void shouldAddTwoNodePropertyStep() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr'})");
        assertCypherResult("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'pageRank', {mutateProperty: 'pr2'})",
            List.of(Map.of("name", "myPipeline",
                "splitConfig", Map.of(),
                "nodePropertySteps", List.of(
                    Map.of(
                        "name", "pageRank",
                        "config", Map.of("mutateProperty", "pr")
                    ),
                    Map.of(
                        "name", "pageRank",
                        "config", Map.of("mutateProperty", "pr2")
                    )),
                "featureSteps", List.of(),
                "parameterSpace", List.of()
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

    @Disabled
    @Test
    void shouldThrowInvalidNodePropertyStepName() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('myPipeline', 'juggleSpoons', {mutateProperty: 'pr'})",
            "Invalid procedure name `juggleSpoons` for pipelining."
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
            TestTrainConfig.of()
        );

        ModelCatalog.set(model1);
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
            TestTrainConfig.of()
        );

        ModelCatalog.set(model1);
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('testModel1', 'pageRank', {mutateProperty: 'pr'})",
            "Steps can only be added to a model of type `Link prediction training pipeline`. But model `testModel1` is of type `testAlgo1`."
        );
    }
}
