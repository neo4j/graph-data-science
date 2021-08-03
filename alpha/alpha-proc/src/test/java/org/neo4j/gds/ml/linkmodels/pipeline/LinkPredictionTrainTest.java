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

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateProc;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class LinkPredictionTrainTest extends BaseProcTest {

    public static final String GRAPH_NAME = "g";
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
        "(n)-[:REL]->(o) ";

    GraphStore graphStore;

    @BeforeEach
    void setup() throws Exception {
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

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();
    }

    @Test
    void trainsAModel() {
        ProcedureTestUtils.applyOnProcedure(db, (Consumer<? super AlgoBaseProc<?, ?, ?>>) caller -> {
            LinkPredictionTrainConfig config = LinkPredictionTrainConfig
                .builder()
                .graphName(GRAPH_NAME)
                .modelName("model")
                .negativeClassWeight(1)
                .holdOutFraction(0.5)
                .negativeSamplingRatio(1)
                .validationFolds(2)
                .randomSeed(1337L)
                .params(List.of(
                    Map.of("penalty", 1000000),
                    Map.of("penalty", 1)
                ))
                .build();


            var featurePipeline = new FeaturePipeline(caller, db.databaseId(), getUsername());
            featurePipeline.addFeature(
                LinkFeatureStepFactory.HADAMARD.name(),
                Map.of("nodeProperties", List.of("noise", "z", "array"))
            );

            var linkPredictionTrain = new LinkPredictionTrain(
                GRAPH_NAME,
                graphStore,
                config,
                featurePipeline,
                ProgressTracker.NULL_TRACKER,
                caller
            );

            var customInfo = (LinkPredictionModelInfo) linkPredictionTrain.compute().customInfo();

            assertThat(customInfo.metrics().get(LinkMetric.AUCPR).validation())
                .hasSize(2)
                .satisfies(scores ->
                    assertThat(scores.get(0).avg()).isNotCloseTo(scores.get(1).avg(), Percentage.withPercentage(0.2))
                );

            assertThat(customInfo.bestParameters())
                .usingRecursiveComparison()
                .isEqualTo(LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1)));
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void seededTrain(int concurrency) {
        ProcedureTestUtils.applyOnProcedure(db, (Consumer<? super AlgoBaseProc<?, ?, ?>>) caller -> {
            LinkPredictionTrainConfig config = LinkPredictionTrainConfig
                .builder()
                .graphName(GRAPH_NAME)
                .modelName("model")
                .negativeClassWeight(1)
                .holdOutFraction(0.5)
                .negativeSamplingRatio(1)
                .validationFolds(2)
                .randomSeed(1337L)
                .params(List.of(Map.of("penalty", 1)))
                .build();

            var featurePipeline = new FeaturePipeline(caller, db.databaseId(), getUsername());
            featurePipeline.addFeature(
                LinkFeatureStepFactory.HADAMARD.name(),
                Map.of("nodeProperties", List.of("noise", "z", "array"))
            );

            var linkPredictionTrain = new LinkPredictionTrain(
                "g",
                graphStore,
                config,
                featurePipeline,
                ProgressTracker.NULL_TRACKER,
                caller
            );

            var modelWeights = linkPredictionTrain.compute().data().weights().data();
            var modelWeightsRepeated = linkPredictionTrain.compute().data().weights().data();

            assertThat(modelWeights).matches(modelWeightsRepeated::equals);
        });
    }
}
