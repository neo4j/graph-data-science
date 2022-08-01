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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.PredictedLink;
import org.neo4j.gds.ml.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;
import org.neo4j.gds.similarity.knn.ImmutableKnnBaseConfig;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ApproximateLinkPredictionTest extends BaseProcTest {
    public static final String GRAPH_NAME = "g";

    @Neo4jGraph
    static String GDL = "CREATE " +
                        "  (n0:N {a: 1.0, b: 0.8, c: 1.0})" +
                        ", (n1:N {a: 2.0, b: 1.0, c: 1.0})" +
                        ", (n2:N {a: 3.0, b: 1.5, c: 1.0})" +
                        ", (n3:N {a: 0.0, b: 2.8, c: 1.0})" +
                        ", (n4:N {a: 1.0, b: 0.9, c: 1.0})" +
                        ", (n1)-[:T]->(n2)" +
                        ", (n3)-[:T]->(n4)" +
                        ", (n1)-[:T]->(n3)" +
                        ", (n2)-[:T]->(n4)";

    private static final double[] WEIGHTS = new double[]{2.0, 1.0, -3.0};

    private Graph graph;

    private GraphStore graphStore;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("a", "b", "c"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "g").graphStore();
        graph = graphStore.getUnion();
    }

    @ParameterizedTest
    @CsvSource(value = {"1, 44, 1", "2, 59, 1"})
    void shouldPredictWithTopK(int topK, long expectedLinksConsidered, int ranIterations) {
        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(
                new Matrix(
                    WEIGHTS,
                    1,
                    WEIGHTS.length
                )),
            Weights.ofVector(0.0)
        );

        var linkPrediction = new ApproximateLinkPrediction(
            LogisticRegressionClassifier.from(modelData),
            LinkFeatureExtractor.of(graph, List.of(new L2FeatureStep(List.of("a", "b", "c")))),
            graph,
            graphStore.getGraph(NodeLabel.of("N")),
            graphStore.getGraph(NodeLabel.of("N")),
            ImmutableKnnBaseConfig.builder()
                .randomSeed(42L)
                .concurrency(1)
                .randomJoins(10)
                .maxIterations(10)
                .sampleRate(0.9)
                .deltaThreshold(0)
                .topK(topK)
                .nodeProperties(List.of(new KnnNodePropertySpec("DUMMY")))
                .build(),
            ProgressTracker.NULL_TRACKER
        );
        var predictionResult = linkPrediction.compute();
        assertThat(predictionResult.samplingStats()).isEqualTo(
            Map.of(
                "strategy", "approximate",
                "linksConsidered", expectedLinksConsidered,
                "ranIterations", ranIterations,
                "didConverge", true
            )
        );

        var predictedLinks = predictionResult.stream().collect(Collectors.toList());

        assertThat(predictedLinks.size()).isLessThanOrEqualTo((int) (topK * graph.nodeCount()));

        var expectedLinks = List.of(
            PredictedLink.of(0, 4, 0.49750002083312506),
            PredictedLink.of(0, 1, 0.1150667320455498),
            PredictedLink.of(1, 4, 0.11815697780926955),
            PredictedLink.of(1, 0, 0.1150667320455498),
            PredictedLink.of(2, 0, 2.054710330936739E-4),
            PredictedLink.of(2, 3, 2.810228605019864E-9),
            PredictedLink.of(3, 0, 0.0024726231566347765),
            PredictedLink.of(3, 2, 2.810228605019864E-9),
            PredictedLink.of(4, 0, 0.49750002083312506),
            PredictedLink.of(4, 1, 0.11815697780926955)
        );

        assertThat(predictedLinks)
            .usingElementComparator(compareWithPrecision(1e-10))
            .isSubsetOf(expectedLinks)
            .allMatch(prediction -> !graph.exists(prediction.sourceId(), prediction.targetId()));

    }

    @Test
    void shouldPredictTwice() {
        double[] weights = {2.0, 1.0, -3.0};

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(new Matrix(
                weights,
                1,
                weights.length
            )),
            Weights.ofVector(0.0)
        );

        var expectedLinks = List.of(
            PredictedLink.of(0, 4, 0.4975000208331247),
            PredictedLink.of(1, 0, 0.1150667320455497),
            PredictedLink.of(2, 0, 2.0547103309367367E-4),
            PredictedLink.of(3, 0, 0.002472623156634774),
            PredictedLink.of(4, 0, 0.4975000208331247)
        );

        for (int i = 0; i < 2; i++) {
            var linkPrediction = new ApproximateLinkPrediction(
                LogisticRegressionClassifier.from(modelData),
                LinkFeatureExtractor.of(graph, List.of(new L2FeatureStep(List.of("a", "b", "c")))),
                graph,
                graphStore.getGraph(NodeLabel.of("N")),
                graphStore.getGraph(NodeLabel.of("N")),
                ImmutableKnnBaseConfig.builder()
                    .randomSeed(42L)
                    .concurrency(1)
                    .randomJoins(10)
                    .maxIterations(10)
                    .sampleRate(0.9)
                    .deltaThreshold(0)
                    .topK(1)
                    .nodeProperties(List.of(new KnnNodePropertySpec("DUMMY")))
                    .build(),
                ProgressTracker.NULL_TRACKER
            );

            var predictionResult = linkPrediction.compute();
            var predictedLinks = predictionResult.stream().collect(Collectors.toList());
            assertThat(predictedLinks).hasSize(5);

            assertThat(predictedLinks)
                .usingElementComparator(compareWithPrecision(1e-10))
                .containsAll(expectedLinks);
        }
    }

    @Test
    void shouldNotPredictExistingLinks() {
        int topK = 50;
        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.addFeatureStep(new L2FeatureStep(List.of("a", "b", "c")));

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(
                new Matrix(
                    WEIGHTS,
                    1,
                    WEIGHTS.length
                )),
            Weights.ofVector(0.0)
        );

        var linkPrediction = new ApproximateLinkPrediction(
            LogisticRegressionClassifier.from(modelData),
            LinkFeatureExtractor.of(graph, List.of(new L2FeatureStep(List.of("a", "b", "c")))),
            graph,
            graphStore.getGraph(NodeLabel.of("N")),
            graphStore.getGraph(NodeLabel.of("N")),
            ImmutableKnnBaseConfig.builder()
                .randomSeed(42L)
                .concurrency(1)
                .randomJoins(10)
                .maxIterations(10)
                .sampleRate(0.9)
                .deltaThreshold(0)
                .topK(topK)
                .nodeProperties(List.of(new KnnNodePropertySpec("DUMMY")))
                .build(),
            ProgressTracker.NULL_TRACKER
        );
        var predictionResult = linkPrediction.compute();

        predictionResult.stream().forEach(predictedLink -> {
            assertThat(graph.exists(predictedLink.sourceId(), predictedLink.targetId())).isFalse();
            assertThat(graph.exists(predictedLink.targetId(), predictedLink.sourceId())).isFalse();
            assertThat(predictedLink.targetId()).isNotEqualTo(predictedLink.sourceId());
        });
    }

    @Test
    void estimate() {
        var config = LinkPredictionPredictPipelineBaseConfigImpl.builder()
            .topK(10)
            .sampleRate(0.1)
            .username("DUMMY")
            .modelName("DUMMY")
            .graphName("DUMMY")
            .sourceNodeLabel("DUMMY")
            .targetNodeLabel("DUMMY")
            .build();

        var actualEstimate = ApproximateLinkPrediction
            .estimate(config)
            .estimate(GraphDimensions.of(100, 1000), config.concurrency());

        assertThat(actualEstimate.memoryUsage().toString()).isEqualTo("[24 KiB ... 43 KiB]");
    }

    static Comparator<PredictedLink> compareWithPrecision(double precision) {
        return (o1, o2) -> {
            boolean sourceEq = o1.sourceId() == o2.sourceId();
            boolean targetEq = o1.targetId() == o2.targetId();
            boolean probEq = Math.abs(o1.probability() - o2.probability()) < precision;
            if (sourceEq && targetEq && probEq) {
                return 0;
            } else return -1;
        };
    }
}
