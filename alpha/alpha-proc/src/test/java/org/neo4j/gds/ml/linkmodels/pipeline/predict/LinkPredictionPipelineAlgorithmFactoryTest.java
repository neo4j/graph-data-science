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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.louvain.LouvainMutateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCreateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.linkmodels.pipeline.train.LinkPredictionTrainFactory;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestLog.INFO;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCreateProc.PIPELINE_MODEL_TYPE;

class LinkPredictionPipelineAlgorithmFactoryTest extends BaseProcTest {
    public static final String GRAPH_NAME = "g";
    public static final String PIPELINE_NAME = "p";
    public GraphStore graphStore;

    // Five cliques of size 2, 3, or 4
    @Neo4jGraph
    static String GRAPH =
        "CREATE " +
        "(a:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(b:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(c:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(d:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(e:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(f:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(g:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(h:N {noise: 42, z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
        "(i:N {noise: 42, z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
        "(j:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(k:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(l:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(m:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(n:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(o:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(p:Ignore {noise: -1, z: -1, array: [1.0]}), " +

        "(a)-[:REL]->(b), " +
        "(a)-[:REL]->(c), " +
        "(a)-[:REL]->(d), " +
        "(b)-[:REL]->(c), " +
        "(b)-[:REL]->(d), " +
        "(c)-[:REL]->(d), " +
        "(e)-[:REL]->(f), " +
        "(e)-[:REL]->(g), " +
        "(f)-[:REL]->(g), " +
        "(h)-[:REL]->(i), " +
        "(j)-[:REL]->(k), " +
        "(j)-[:REL]->(l), " +
        "(k)-[:REL]->(l), " +
        "(m)-[:REL]->(n), " +
        "(m)-[:REL]->(o), " +
        "(n)-[:REL]->(o), " +
        "(a)-[:REL]->(p), " +

        "(a)-[:IGNORED]->(e), " +
        "(m)-[:IGNORED]->(a), " +
        "(m)-[:IGNORED]->(b), " +
        "(m)-[:IGNORED]->(c) ";

    public static Stream<Arguments> configTaskTreeMappings() {
        return Stream.of(
            Arguments.of(
                Map.of(
                    "modelName", "outputModel",
                    "topN", 6,
                    "mutateRelationshipType", "PREDICTED"
                ),
                List.of(
                    "Link Prediction Pipeline :: exhaustive link prediction :: Start",
                    "Link Prediction Pipeline :: exhaustive link prediction 100%",
                    "Link Prediction Pipeline :: exhaustive link prediction :: Finished"
                )
            ),
            Arguments.of(
                Map.of(
                    "modelName", "outputModel",
                    "sampleRate", 0.4,
                    "mutateRelationshipType", "PREDICTED",
                    "randomJoins", 0,
                    "maxIterations", 1
                ),
                List.of(
                    "Link Prediction Pipeline :: approximate link prediction :: Start",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Start",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Initialize random neighbors :: Start",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Initialize random neighbors 6%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Initialize random neighbors 100%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Initialize random neighbors :: Finished",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: KNN-G Graph init took `some time`",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Start",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Split old and new neighbors 1 of 1 :: Start",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Split old and new neighbors 1 of 1 6%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Split old and new neighbors 1 of 1 13%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Split old and new neighbors 1 of 1 20%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Split old and new neighbors 1 of 1 26%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Split old and new neighbors 1 of 1 100%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Split old and new neighbors 1 of 1 :: Finished",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 :: Start",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 6%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 13%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 20%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 26%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 33%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 40%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 46%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 53%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 60%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 66%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 73%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 80%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 86%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 93%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 100%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Reverse old and new neighbors 1 of 1 :: Finished",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Join neighbors 1 of 1 :: Start",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Join neighbors 1 of 1 6%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Join neighbors 1 of 1 100%",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Join neighbors 1 of 1 :: Finished",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: KNN-G Graph iteration 1 took `some time`",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Iteration :: Finished",
                    "Link Prediction Pipeline :: approximate link prediction :: Knn :: Finished",
                    "Link Prediction Pipeline :: approximate link prediction :: KNN-G Graph execution took `some time`",
                    "Link Prediction Pipeline :: approximate link prediction :: Finished"
                )
            )

        );
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class);

        String createQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();
    }

    @AfterEach
    void tearDown(){
        ModelCatalog.removeAllLoadedModels();
    }

    @ParameterizedTest
    @MethodSource("configTaskTreeMappings")
    void progressTracking(Map<String,Object> predictParameters,List<String> expectedPredictLogEntries ) {
        TestProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            String trainModelName = "outputModel";
            var trainConfig = LinkPredictionTrainConfig.builder()
                .graphName(GRAPH_NAME)
                .pipeline(PIPELINE_NAME)
                .modelName(trainModelName)
                .concurrency(1)
                .randomSeed(42L)
                .build();

            LinkPredictionPipeline pipeline = new LinkPredictionPipeline();
            pipeline.addNodePropertyStep(NodePropertyStep.of("degree", Map.of("mutateProperty", "degree")));
            pipeline.addNodePropertyStep(NodePropertyStep.of("pageRank", Map.of("mutateProperty", "pr")));
            pipeline.addFeatureStep(new HadamardFeatureStep(List.of("noise", "z", "array", "degree", "pr")));
            pipeline.setParameterSpace(List.of(Map.of("penalty", 1000000), Map.of("penalty", 1)));

            var pipeModel = Model.of(
                getUsername(),
                PIPELINE_NAME,
                PIPELINE_MODEL_TYPE,
                GraphSchema.empty(),
                new Object(),
                LinkPredictionPipelineCreateProc.PipelineDummyTrainConfig.of(getUsername()),
                pipeline
            );
            ModelCatalog.set(pipeModel);

            var trainAlgo = new LinkPredictionTrainFactory(db.databaseId(), caller).build(
                graphStore.getUnion(),
                trainConfig,
                AllocationTracker.empty(),
                ProgressTracker.NULL_TRACKER
            );
            var trainedModel = trainAlgo.compute();
            ModelCatalog.set(trainedModel);

            var predictConfig = new LinkPredictionPipelineMutateConfigImpl(
                Optional.of(GRAPH_NAME),
                Optional.empty(),
                "",
                CypherMapWrapper.create(predictParameters)
            );

            var factory = new LinkPredictionPipelineAlgorithmFactory<LinkPredictionPipelineMutateConfig>(
                caller,
                db.databaseId()
            );

            var progressTask = factory.progressTask(graphStore.getUnion(), predictConfig);
            var log = new TestLog();
            var progressTracker = new TestProgressTracker(
                progressTask,
                log,
                1,
                EmptyTaskRegistryFactory.INSTANCE
            );

            var predictAlgo = factory.build(
                graphStore.getUnion(),
                predictConfig,
                AllocationTracker.empty(),
                progressTracker
            );

            predictAlgo.compute();

            var expectedMessages = new ArrayList<String>();
            expectedMessages.addAll(List.of(
                "Link Prediction Pipeline :: Start",
                "Link Prediction Pipeline :: execute node property steps :: Start",
                "Link Prediction Pipeline :: execute node property steps :: step 1 of 2 :: Start",
                "Link Prediction Pipeline :: execute node property steps :: step 1 of 2 100%",
                "Link Prediction Pipeline :: execute node property steps :: step 1 of 2 :: Finished",
                "Link Prediction Pipeline :: execute node property steps :: step 2 of 2 :: Start",
                "Link Prediction Pipeline :: execute node property steps :: step 2 of 2 100%",
                "Link Prediction Pipeline :: execute node property steps :: step 2 of 2 :: Finished",
                "Link Prediction Pipeline :: execute node property steps :: Finished"
            ));
            expectedMessages.addAll(expectedPredictLogEntries);
            expectedMessages.add("Link Prediction Pipeline :: Finished");

            assertThat(log.getMessages(INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(expectedMessages.toArray(String[]::new));
        });
    }
}
