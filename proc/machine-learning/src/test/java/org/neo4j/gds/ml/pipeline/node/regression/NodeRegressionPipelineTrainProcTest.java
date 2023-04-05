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
package org.neo4j.gds.ml.pipeline.node.regression;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.MapAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineAddStepProcs;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineAddTrainerMethodProcs;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineConfigureSplitProc;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineCreateProc;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;
import org.neo4j.gds.model.catalog.ModelDropProc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.aMapWithSize;

@Neo4jModelCatalogExtension
class NodeRegressionPipelineTrainProcTest extends BaseProcTest {

    private static final String GRAPH_NAME = "g";
    private static final String MODEL_NAME = "model";

    @Neo4jGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (a1:N {scalar: 1.2, array: [1.0, -1.0], t: 2.4})" +
        ", (a2:N {scalar: 0.5, array: [1.0, -1.0], t: 1.0})" +
        ", (a3:N {scalar: 1.1, array: [1.0, -1.0], t: -1})" +
        ", (a4:N {scalar: 0.8, array: [1.0, -1.0], t: 1.6})" +
        ", (a5:N {scalar: 1.3, array: [1.0, -1.0], t: 2.6})" +
        ", (a6:N {scalar: 1.0, array: [2.0, -1.0], t: 2.0})" +
        ", (a7:N {scalar: 0.8, array: [2.0, -1.0], t: 1.6})" +
        ", (a8:N {scalar: 1.5, array: [2.0, -1.0], t: 3.0})" +
        ", (a9:N {scalar: 0.5, array: [2.0, -1.0], t: 1.0})" +
        ", (a1)-[:R]->(a2)" +
        ", (a1)-[:R]->(a4)" +
        ", (a3)-[:R]->(a5)" +
        ", (a5)-[:R]->(a8)" +
        ", (a4)-[:R]->(a6)" +
        ", (a4)-[:R]->(a9)" +
        ", (a2)-[:R]->(a8)";
    public static final String PIPELINE_NAME = "pipe";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            ModelDropProc.class,
            NodeRegressionPipelineCreateProc.class,
            NodeRegressionPipelineAddStepProcs.class,
            NodeRegressionPipelineAddTrainerMethodProcs.class,
            NodeRegressionPipelineConfigureSplitProc.class,
            NodeRegressionPipelineTrainProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withRelationshipType("R")
            .withNodeProperties(List.of("array", "scalar", "t"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);
    }


    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void failsOnInvalidTargetProperty() {
        var pipe = Map.<String, Object>of("pipeline", PIPELINE_NAME);

        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.create($pipeline)",
            pipe
        );
        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.selectFeatures($pipeline, ['array', 'scalar'])",
            pipe
        );
        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.addLinearRegression($pipeline, {penalty: 1000, maxEpochs: 1})",
            pipe
        );
        var pipeline = new NodeRegressionTrainingPipeline();
        pipeline.featureProperties().add("array");

        var params = new HashMap<>(pipe);
        params.put("graphName", GRAPH_NAME);
        params.put("modelName", MODEL_NAME);
        assertThatThrownBy(() -> runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.train(" +
            "   $graphName, {" +
            "       pipeline: $pipeline," +
            "       modelName: $modelName," +
            "       targetProperty: 'INVALID_PROPERTY'," +
            "       metrics: ['ROOT_MEAN_SQUARED_ERROR']," +
            "       randomSeed: 1" +
            "})",
            params
        ))
            .getRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Target property `INVALID_PROPERTY` not found in graph with node properties:");
    }

    @Test
    void train() {
        var pipe = Map.<String, Object>of("pipeline", PIPELINE_NAME);

        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.create($pipeline)",
            pipe
        );
        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.addNodeProperty($pipeline, 'pageRank', {mutateProperty: 'pr'})",
            pipe
        );
        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.selectFeatures($pipeline, ['array', 'scalar', 'pr'])",
            pipe
        );
        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.addLinearRegression($pipeline, {penalty: 1000, maxEpochs: 1})",
            pipe
        );
        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.configureSplit($pipeline, {testFraction: 0.3, validationFolds: 2})",
            pipe
        );

        var params = new HashMap<>(pipe);
        params.put("graphName", GRAPH_NAME);
        params.put("modelName", MODEL_NAME);

        var stringObjectMapType = InstanceOfAssertFactories.map(
            String.class,
            Object.class
        );

        var modelInfoCheck = new Condition<>(m -> {
            var modelInfo = assertThat(m).asInstanceOf(stringObjectMapType)
                .containsEntry("modelName", MODEL_NAME)
                .containsEntry("modelType", "NodeRegression")
                .containsKey("bestParameters");

            var featurePipeline = modelInfo.extractingByKey("pipeline", stringObjectMapType);

            Consumer<MapAssert<String, Object>> nodePropertyStepCheck = (MapAssert<String, Object> holder) -> holder
                .extractingByKey("nodePropertySteps", InstanceOfAssertFactories.LIST)
                .hasSize(1)
                .element(0, stringObjectMapType)
                .containsEntry("name", "gds.pageRank.mutate")
                .containsEntry("config", Map.of("mutateProperty", "pr", "contextNodeLabels", List.of(), "contextRelationshipTypes", List.of()));
            nodePropertyStepCheck.accept(featurePipeline);
            nodePropertyStepCheck.accept(modelInfo);

            featurePipeline
                .extractingByKey("featureProperties", InstanceOfAssertFactories.list(Map.class))
                .extracting(map -> map.get("feature"))
                .containsExactly("array", "scalar", "pr");

            modelInfo
                .extractingByKey("featureProperties", InstanceOfAssertFactories.list(String.class))
                .containsExactly("array", "scalar", "pr");

            return true;
        }, "a modelInfo map");

        var modelSelectionStatsCheck = new Condition<>(mss -> {
            assertThat(mss).asInstanceOf(stringObjectMapType)
                .containsKeys("bestParameters", "modelCandidates");
            return true;
        }, "a model selection statistics map");

        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.train(" +
            "   $graphName, {" +
            "       pipeline: $pipeline," +
            "       modelName: $modelName," +
            "       targetProperty: 't'," +
            "       metrics: ['ROOT_MEAN_SQUARED_ERROR']," +
            "       randomSeed: 1" +
            "})",
            params,
            List.of(
                Map.of(
                    "modelInfo", modelInfoCheck,
                    "trainMillis", Matchers.greaterThan(-1L),
                    "configuration", Matchers.allOf(
                        Matchers.hasEntry("pipeline", PIPELINE_NAME),
                        Matchers.hasEntry("modelName", MODEL_NAME),
                        aMapWithSize(13)
                    ),
                    "modelSelectionStats",modelSelectionStatsCheck
                )
            )
        );

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), GRAPH_NAME).graphStore();

        Assertions.assertThat(graphStore.nodePropertyKeys(NodeLabel.of("N"))).doesNotContain("pr");
        Assertions.assertThat(graphStore.nodePropertyKeys(NodeLabel.of("Ignore"))).doesNotContain("pr");
    }
}
