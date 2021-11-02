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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.louvain.LouvainMutateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCreateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateProc;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestLog.INFO;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCreateProc.PIPELINE_MODEL_TYPE;

public class LinkPredictionTrainFactoryTest extends BaseProcTest {
    public static final String GRAPH_NAME = "g";
    public static final String PIPELINE_NAME = "p";
    public Graph graph;

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

    private ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        modelCatalog = OpenModelCatalog.INSTANCE;
        registerProcedures(
            GraphCreateProc.class,
            SplitRelationshipsMutateProc.class
        );

        String createQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(createQuery);

        graph = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore().getUnion();
    }

    @AfterEach
    void tearDown() {
        modelCatalog.removeAllLoadedModels();
    }

    @Test
    void progressTracking() {
        TestProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var config = LinkPredictionTrainConfig.builder()
                .graphName(GRAPH_NAME)
                .pipeline(PIPELINE_NAME)
                .modelName("outputModel")
                .concurrency(1)
                .randomSeed(42L)
                .build();

            LinkPredictionPipeline pipeline = new LinkPredictionPipeline();
            pipeline.addNodePropertyStep(NodePropertyStep.of("degree", Map.of("mutateProperty", "degree")));
            pipeline.addNodePropertyStep(NodePropertyStep.of("pageRank", Map.of("mutateProperty", "pr")));
            pipeline.addFeatureStep(new HadamardFeatureStep(List.of("noise", "z", "array", "degree", "pr")));
            pipeline.setParameterSpace(Stream.<Map<String, Object>>of(
                    Map.of("penalty", 1000000),
                    Map.of("penalty", 1)
                )
                .map(LinkLogisticRegressionTrainConfig::of)
                .collect(Collectors.toList()));

            var pipeModel = Model.of(
                getUsername(),
                PIPELINE_NAME,
                PIPELINE_MODEL_TYPE,
                GraphSchema.empty(),
                new Object(),
                LinkPredictionPipelineCreateProc.PipelineDummyTrainConfig.of(getUsername()),
                pipeline
            );
            modelCatalog.set(pipeModel);

            var factory = new LinkPredictionTrainFactory(db.databaseId(), caller);

            var progressTask = factory.progressTask(graph, config);
            var log = new TestLog();
            var progressTracker = new TestProgressTracker(
                progressTask,
                log,
                1,
                EmptyTaskRegistryFactory.INSTANCE
            );

            var linkPredictionTrain = factory.build(
                graph,
                config,
                AllocationTracker.empty(),
                progressTracker
            );

            linkPredictionTrain.compute();

            assertThat(log.getMessages(INFO))
                .extracting(removingThreadId())
                .containsExactly(
                    "Link Prediction pipeline train :: Start",
                    "Link Prediction pipeline train :: split relationships :: Start",
                    "Link Prediction pipeline train :: split relationships 100%",
                    "Link Prediction pipeline train :: split relationships :: Finished",
                    "Link Prediction pipeline train :: execute node property steps :: Start",
                    "Link Prediction pipeline train :: execute node property steps :: step 1 of 2 :: Start",
                    "Link Prediction pipeline train :: execute node property steps :: step 1 of 2 100%",
                    "Link Prediction pipeline train :: execute node property steps :: step 1 of 2 :: Finished",
                    "Link Prediction pipeline train :: execute node property steps :: step 2 of 2 :: Start",
                    "Link Prediction pipeline train :: execute node property steps :: step 2 of 2 100%",
                    "Link Prediction pipeline train :: execute node property steps :: step 2 of 2 :: Finished",
                    "Link Prediction pipeline train :: execute node property steps :: Finished",
                    "Link Prediction pipeline train :: extract train features :: Start",
                    "Link Prediction pipeline train :: extract train features 100%",
                    "Link Prediction pipeline train :: extract train features :: Finished",
                    "Link Prediction pipeline train :: select model :: Start",
                    "Link Prediction pipeline train :: select model 50%",
                    "Link Prediction pipeline train :: select model 100%",
                    "Link Prediction pipeline train :: select model :: Finished",
                    "Link Prediction pipeline train :: train best model :: Start",
                    "Link Prediction pipeline train :: train best model :: Epoch 1 :: Start",
                    "Link Prediction pipeline train :: train best model :: Epoch 1 :: Loss: 3.9999999420000014",
                    "Link Prediction pipeline train :: train best model :: Epoch 1 100%",
                    "Link Prediction pipeline train :: train best model :: Epoch 1 :: Finished",
                    "Link Prediction pipeline train :: train best model :: Epoch 2 :: Start",
                    "Link Prediction pipeline train :: train best model :: Epoch 2 :: Loss: 4.427497785339087",
                    "Link Prediction pipeline train :: train best model :: Epoch 2 100%",
                    "Link Prediction pipeline train :: train best model :: Epoch 2 :: Finished",
                    "Link Prediction pipeline train :: train best model :: converged after 2 epochs. Initial loss: 0.6931471805599453, Last loss: 4.427497785339087.",
                    "Link Prediction pipeline train :: train best model :: Finished",
                    "Link Prediction pipeline train :: compute train metrics :: Start",
                    "Link Prediction pipeline train :: compute train metrics 100%",
                    "Link Prediction pipeline train :: compute train metrics :: Finished",
                    "Link Prediction pipeline train :: evaluate on test data :: Start",
                    "Link Prediction pipeline train :: evaluate on test data :: extract test features :: Start",
                    "Link Prediction pipeline train :: evaluate on test data :: extract test features 100%",
                    "Link Prediction pipeline train :: evaluate on test data :: extract test features :: Finished",
                    "Link Prediction pipeline train :: evaluate on test data :: compute test metrics :: Start",
                    "Link Prediction pipeline train :: evaluate on test data :: compute test metrics 100%",
                    "Link Prediction pipeline train :: evaluate on test data :: compute test metrics :: Finished",
                    "Link Prediction pipeline train :: evaluate on test data :: Finished",
                    "Link Prediction pipeline train :: Finished"
                );
        });
    }
}
