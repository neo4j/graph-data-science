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
package org.neo4j.gds.ml.nodemodels.pipeline.predict;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineAddStepProcs;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineConfigureParamsProc;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineConfigureSplitProc;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCreateProc;
import org.neo4j.gds.model.catalog.ModelDropProc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;

@Neo4jModelCatalogExtension
class NodeClassificationPipelineTrainProcTest extends BaseProcTest {

    private static final String GRAPH_NAME = "g";
    private static final String MODEL_NAME = "model";

    @Neo4jGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (a1:N {scalar: 1.2, array: [1.0, -1.0], t: 0})" +
        ", (a2:N {scalar: 0.5, array: [1.0, -1.0], t: 0})" +
        ", (a3:N {scalar: 1.1, array: [1.0, -1.0], t: 0})" +
        ", (a4:N {scalar: 0.8, array: [1.0, -1.0], t: 0})" +
        ", (a5:N {scalar: 1.3, array: [1.0, -1.0], t: 1})" +
        ", (a6:N {scalar: 1.0, array: [2.0, -1.0], t: 1})" +
        ", (a7:N {scalar: 0.8, array: [2.0, -1.0], t: 1})" +
        ", (a8:N {scalar: 1.5, array: [2.0, -1.0], t: 1})" +
        ", (a9:N {scalar: 0.5, array: [2.0, -1.0], t: 1})" +
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
            GraphCreateProc.class,
            ModelDropProc.class,
            NodeClassificationPipelineCreateProc.class,
            NodeClassificationPipelineAddStepProcs.class,
            NodeClassificationPipelineConfigureParamsProc.class,
            NodeClassificationPipelineConfigureSplitProc.class,
            NodeClassificationPipelineTrainProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphCreate()
            .withNodeLabel("N")
            .withRelationshipType("R")
            .withNodeProperties(List.of("array", "scalar", "t"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);
    }


    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void train() {
        var pipe = Map.<String, Object>of("pipeline", PIPELINE_NAME);

        runQuery(
            "CALL gds.alpha.ml.pipeline.nodeClassification.create($pipeline)",
            pipe
        );
        runQuery(
            "CALL gds.alpha.ml.pipeline.nodeClassification.addNodeProperty($pipeline, 'pageRank', {mutateProperty: 'pr'})",
            pipe
        );
        runQuery(
            "CALL gds.alpha.ml.pipeline.nodeClassification.selectFeatures($pipeline, ['array', 'scalar', 'pr'])",
            pipe
        );
        runQuery(
            "CALL gds.alpha.ml.pipeline.nodeClassification.configureParams($pipeline, [{penalty: 1000, maxEpochs: 1}])",
            pipe
        );
        runQuery(
            "CALL gds.alpha.ml.pipeline.nodeClassification.configureSplit($pipeline, {testFraction: 0.01, validationFolds: 2})",
            pipe
        );

        var params = new HashMap<>(pipe);
        params.put("graphName", GRAPH_NAME);
        params.put("modelName", MODEL_NAME);

        var soMap = InstanceOfAssertFactories.map(
            String.class,
            Object.class
        );

        var modelInfoCheck = new Condition<>(m -> {

            var modelInfo = assertThat(m).asInstanceOf(soMap)
                .containsEntry("modelName", MODEL_NAME)
                .containsEntry("modelType", "Node classification pipeline")
                .containsKey("bestParameters")
                .containsEntry("classes", List.of(0L, 1L));

            var metrics = modelInfo
                .extractingByKey("metrics", soMap)
                .extractingByKey("F1_class_1", soMap)
                .containsKey("validation")
                .containsKey("train");

            metrics.extractingByKey("outerTrain", InstanceOfAssertFactories.DOUBLE)
                .isBetween(0.0, 1.0);

            metrics.extractingByKey("test", InstanceOfAssertFactories.DOUBLE)
                .isBetween(0.0, 1.0);

            var featurePipeline = modelInfo.extractingByKey("trainingPipeline", soMap)
                .containsKey("splitConfig")
                .containsKey("trainingParameterSpace")
                .extractingByKey("featurePipeline", soMap);

            featurePipeline
                .extractingByKey("nodePropertySteps", InstanceOfAssertFactories.LIST)
                .hasSize(1)
                .element(0, soMap)
                .containsEntry("name", "gds.pageRank.mutate")
                .containsEntry("config", Map.of("mutateProperty", "pr"));

            featurePipeline
                .extractingByKey("featureProperties", InstanceOfAssertFactories.list(Map.class))
                .extracting(map -> map.get("feature"))
                .containsExactly("array", "scalar", "pr");

            return true;
        }, "a modelInfo map");


        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.nodeClassification.train(" +
            "   $graphName, {" +
            "       pipeline: $pipeline," +
            "       modelName: $modelName," +
            "       targetProperty: 't'," +
            "       metrics: ['F1(class=1)']," +
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
                        aMapWithSize(10)
                    )
                )
            )
        );

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();

        Assertions.assertThat(graphStore.nodePropertyKeys(NodeLabel.of("N"))).doesNotContain("pr");
        Assertions.assertThat(graphStore.nodePropertyKeys(NodeLabel.of("Ignore"))).doesNotContain("pr");
    }
}
