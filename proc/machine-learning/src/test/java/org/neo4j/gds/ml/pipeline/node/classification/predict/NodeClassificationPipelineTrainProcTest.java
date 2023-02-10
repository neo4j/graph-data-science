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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.MapAssert;
import org.assertj.core.util.DoubleComparator;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineAddStepProcs;
import org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineAddTrainerMethodProcs;
import org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineConfigureSplitProc;
import org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineCreateProc;
import org.neo4j.gds.model.catalog.ModelDropProc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.neo4j.gds.TestSupport.assertCypherMemoryEstimation;

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

    static Stream<Arguments> graphNameOrConfigurations() {
        return Stream.of(
            Arguments.of(
                GRAPH_NAME,
                MemoryRange.of(887_016, 918_976)
            ),
            Arguments.of(
                Map.of("nodeProjection", "*", "relationshipProjection", "*"),
                MemoryRange.of(1_182_408, 1_214_368)
            )
        );
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            ModelDropProc.class,
            NodeClassificationPipelineCreateProc.class,
            NodeClassificationPipelineAddStepProcs.class,
            NodeClassificationPipelineAddTrainerMethodProcs.class,
            NodeClassificationPipelineConfigureSplitProc.class,
            NodeClassificationPipelineTrainProc.class
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
            "CALL gds.beta.pipeline.nodeClassification.create($pipeline)",
            pipe
        );
        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.selectFeatures($pipeline, ['array', 'scalar'])",
            pipe
        );
        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.addLogisticRegression($pipeline, {penalty: 1000, maxEpochs: 1})",
            pipe
        );

        var params = new HashMap<>(pipe);
        params.put("graphName", GRAPH_NAME);
        params.put("modelName", MODEL_NAME);

        assertThatThrownBy(() -> runQuery(
            "CALL gds.beta.pipeline.nodeClassification.train(" +
            "   $graphName, {" +
            "       pipeline: $pipeline," +
            "       modelName: $modelName," +
            "       targetProperty: 'INVALID_PROPERTY'," +
            "       metrics: ['F1(class=1)', 'OUT_OF_BAG_ERROR']," +
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
            "CALL gds.beta.pipeline.nodeClassification.create($pipeline)",
            pipe
        );
        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.addNodeProperty($pipeline, 'pageRank', {mutateProperty: 'pr'})",
            pipe
        );
        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.selectFeatures($pipeline, ['array', 'scalar', 'pr'])",
            pipe
        );
        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.addLogisticRegression($pipeline, {penalty: 1000, maxEpochs: 1})",
            pipe
        );
        runQuery(
            "CALL gds.alpha.pipeline.nodeClassification.addRandomForest($pipeline, {numberOfDecisionTrees: 1})",
            pipe
        );
        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.configureSplit($pipeline, {testFraction: 0.3, validationFolds: 2})",
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
                .containsEntry("modelType", "NodeClassification")
                .containsKey("bestParameters")
                .containsEntry("classes", List.of(0L, 1L));

            modelInfo
                .extractingByKey("metrics", soMap)
                .extractingByKey("F1_class_1", soMap)
                .usingRecursiveComparison()
                .withComparatorForType(new DoubleComparator(1e-6), Double.class)
                .isEqualTo(Map.of(
                    "validation", Map.of("avg", 0.799999992, "max", 0.799999992, "min", 0.799999992),
                    "train", Map.of("avg", 0.799999992, "max", 0.799999992, "min", 0.799999992),
                    "outerTrain",0.7999999936,
                    "test",0.49999999375000004
                ));

            var featurePipeline = modelInfo.extractingByKey("pipeline", soMap);

            Consumer<MapAssert<String, Object>> nodePropertyStepCheck = (MapAssert<String, Object> holder) -> holder
                .extractingByKey("nodePropertySteps", InstanceOfAssertFactories.LIST)
                .hasSize(1)
                .element(0, soMap)
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
            assertThat(mss).asInstanceOf(soMap)
                .containsKeys("bestParameters", "modelCandidates");
            return true;
        }, "a model selection statistics map");

        assertCypherResult(
            "CALL gds.beta.pipeline.nodeClassification.train(" +
            "   $graphName, {" +
            "       pipeline: $pipeline," +
            "       modelName: $modelName," +
            "       targetProperty: 't'," +
            "       metrics: ['F1(class=1)', 'OUT_OF_BAG_ERROR']," +
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
                        aMapWithSize(12)
                    ),
                    "modelSelectionStats", modelSelectionStatsCheck
                )
            )
        );

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), GRAPH_NAME).graphStore();

        Assertions.assertThat(graphStore.nodePropertyKeys(NodeLabel.of("N"))).doesNotContain("pr");
        Assertions.assertThat(graphStore.nodePropertyKeys(NodeLabel.of("Ignore"))).doesNotContain("pr");
    }

    @ParameterizedTest
    @MethodSource("graphNameOrConfigurations")
    void shouldEstimateMemory(Object graphNameOrConfiguration, MemoryRange expected) {
        var pipe = Map.<String, Object>of("pipeline", PIPELINE_NAME);

        var params = new HashMap<>(pipe);
        params.put("graphDefinition", graphNameOrConfiguration);
        params.put("modelName", MODEL_NAME);

        runQuery("CALL gds.beta.pipeline.nodeClassification.create($pipeline)", pipe);
        runQuery("CALL gds.beta.pipeline.nodeClassification.addLogisticRegression($pipeline)", pipe);

        assertCypherMemoryEstimation(
            db,
            "CALL gds.beta.pipeline.nodeClassification.train.estimate(" +
            "   $graphDefinition, {" +
            "       pipeline: $pipeline," +
            "       modelName: $modelName," +
            "       targetProperty: 't'," +
            "       metrics: ['F1(class=1)']," +
            "       randomSeed: 1" +
            "})" +
            "YIELD bytesMin, bytesMax, nodeCount, relationshipCount",
            params,
            expected,
            9,
            7
        );
    }

    @Test
    void cannotUseOOBAsMainMetricWithLR() {
        var pipe = Map.<String, Object>of("pipeline", PIPELINE_NAME);

        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.create($pipeline)",
            pipe
        );
        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.selectFeatures($pipeline, ['scalar'])",
            pipe
        );
        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.addLogisticRegression($pipeline, {penalty: 1000, maxEpochs: 1})",
            pipe
        );
        runQuery(
            "CALL gds.alpha.pipeline.nodeClassification.addRandomForest($pipeline, {numberOfDecisionTrees: 1})",
            pipe
        );
        assertError(
            "CALL gds.beta.pipeline.nodeClassification.train(" +
            "   $graphName, {" +
            "       pipeline: $pipeline," +
            "       modelName: $modelName," +
            "       targetProperty: 't'," +
            "       metrics: ['OUT_OF_BAG_ERROR', 'F1(class=1)']," +
            "       randomSeed: 1" +
            "})",
            Map.of("graphName", GRAPH_NAME, "pipeline", PIPELINE_NAME, "modelName", "anything"),
            "If OUT_OF_BAG_ERROR is used as the main metric (the first one)," +
            " then only RandomForest model candidates are allowed." +
            " Incompatible training methods used are: ['LogisticRegression']"
        );
    }

    @Test
    void useOOBAsMainMetricWithRF() {
        var pipe = Map.<String, Object>of("pipeline", PIPELINE_NAME);

        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.create($pipeline)",
            pipe
        );
        runQuery(
            "CALL gds.beta.pipeline.nodeClassification.selectFeatures($pipeline, ['scalar'])",
            pipe
        );
        runQuery(
            "CALL gds.alpha.pipeline.nodeClassification.addRandomForest($pipeline, {numberOfDecisionTrees: 1})",
            pipe
        );
        assertCypherResult(
            "CALL gds.beta.pipeline.nodeClassification.train(" +
            "   $graphName, {" +
            "       pipeline: $pipeline," +
            "       modelName: $modelName," +
            "       targetProperty: 't'," +
            "       metrics: ['OUT_OF_BAG_ERROR', 'F1(class=1)']," +
            "       randomSeed: 1" +
            "}) YIELD modelInfo" +
            " RETURN modelInfo.metrics.OUT_OF_BAG_ERROR AS OOB",
            Map.of("graphName", GRAPH_NAME, "pipeline", PIPELINE_NAME, "modelName", "anything"),
            List.of(Map.of("OOB", Map.of(
                "test", 1.0,
                "validation", Map.of(
                        "avg", 0.3333333333333333,
                        "min", 0.0,
                        "max", 1.0
                    )
                )
            ))

        );
    }
}
