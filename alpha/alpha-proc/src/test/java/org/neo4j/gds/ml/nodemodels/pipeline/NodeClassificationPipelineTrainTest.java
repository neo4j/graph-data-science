/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.louvain.LouvainMutateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCreateProc.PipelineDummyTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineTrain.MODEL_TYPE;

class NodeClassificationPipelineTrainTest extends BaseProcTest {
    private static String PIPELINE_NAME = "pipe";
    private static final String GRAPH_NAME = "g";

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

    private GraphStore graphStore;

    private final ModelCatalog modelCatalog = OpenModelCatalog.INSTANCE;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class);

        String createQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withRelationshipType("R")
            .withNodeProperties(List.of("array", "scalar", "t"), DefaultValue.DEFAULT)
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();
    }

    @AfterEach
    void tearDown() {
        modelCatalog.removeAllLoadedModels();
    }

    @Test
    void trainsAModel() {
        var pipeline = insertPipelineIntoCatalog();
        pipeline.nodePropertySteps().add(NodePropertyStep.of("pageRank", Map.of("mutateProperty", "pr")));
        pipeline.featureProperties().addAll(List.of("array", "scalar", "pr"));

        var metricSpecification = MetricSpecification.parse("F1(class=1)");
        var metric = metricSpecification.createMetrics(List.of()).findFirst().get();

        pipeline.setParameterSpace(List.of(Map.of("penalty", 1000, "maxEpochs", 1)));

        pipeline.setSplitConfig(ImmutableNodeClassificationSplitConfig.builder()
            .holdoutFraction(0.01)
            .validationFolds(2)
            .build()
        );

        var config = createConfig(
            "model",
            metricSpecification,
            1L
        );

        TestProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var ncPipeTrain = new NodeClassificationPipelineTrain(
                config,
                graphStore,
                GRAPH_NAME,
                AllocationTracker.empty(),
                ProgressTracker.NULL_TRACKER,
                caller,
                getUsername()
            );

            var model = ncPipeTrain.compute();

            // using explicit type intentionally :)
            NodeClassificationPipelineModelInfo customInfo = model.customInfo();
            assertThat(customInfo.metrics().get(metric).validation()).hasSize(1);
            assertThat(customInfo.metrics().get(metric).train()).hasSize(1);

            assertThat(customInfo.trainingPipeline()).isEqualTo(pipeline);
        });
    }

    private NodeClassificationTrainingPipeline insertPipelineIntoCatalog() {
        var dummyConfig = PipelineDummyTrainConfig.of(getUsername());
        var info = new NodeClassificationTrainingPipeline();
        modelCatalog.set(
            Model.of("", PIPELINE_NAME, MODEL_TYPE, GraphSchema.empty(), new Object(), dummyConfig, info)
        );
        return info;
    }

    private NodeClassificationPipelineTrainConfig createConfig(
        String modelName,
        MetricSpecification metricSpecification,
        long randomSeed
    ) {
        return ImmutableNodeClassificationPipelineTrainConfig.builder()
            .pipeline(PIPELINE_NAME)
            .modelName(modelName)
            .concurrency(1)
            .randomSeed(randomSeed)
            .targetProperty("t")
            .metrics(List.of(metricSpecification))
            .build();
    }
}
