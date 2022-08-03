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
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.PredictedLink;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.L2FeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.ImmutableLinkLogisticRegressionData;
import org.neo4j.gds.similarity.knn.ImmutableKnnBaseConfig;

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

    private static final double[] WEIGHTS = new double[]{-2.0, -1.0, 3.0};

    private Graph graph;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("a", "b", "c"), DefaultValue.DEFAULT)
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(createQuery);

        var graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), "g").graphStore();
        graph = graphStore.getUnion();
    }

    @ParameterizedTest
    @CsvSource(value = {"1, 75, 2", "2, 89, 1"})
    void shouldPredictWithTopK(int topK, long expectedLinksConsidered, int ranIterations) {
        var modelData = ImmutableLinkLogisticRegressionData.of(
            new Weights<>(
                new Matrix(
                    WEIGHTS,
                    1,
                    WEIGHTS.length
                )),
            Weights.ofScalar(0)
        );

        var linkPrediction = new ApproximateLinkPrediction(
            modelData,
            LinkFeatureExtractor.of(graph, List.of(new L2FeatureStep(List.of("a", "b", "c")))),
            graph,
            ImmutableKnnBaseConfig.builder()
                .randomSeed(42L)
                .concurrency(1)
                .randomJoins(10)
                .maxIterations(10)
                .sampleRate(0.9)
                .deltaThreshold(0)
                .topK(topK)
                .nodeWeightProperty("DUMMY")
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
                PredictedLink.of(0, 3, 0.0024726231566347774),
                PredictedLink.of(0, 1, 0.11506673204554983),
                PredictedLink.of(1, 0, 0.11506673204554983),
                PredictedLink.of(1, 4, 0.11815697780926958),
                PredictedLink.of(1, 2, 0.0953494648991095),
                PredictedLink.of(2, 1, 0.0953494648991095),
                PredictedLink.of(2, 3, 2.810228605019867E-9),
                PredictedLink.of(2, 0, 2.0547103309367397E-4),
                PredictedLink.of(3, 0, 0.0024726231566347774),
                PredictedLink.of(3, 2, 2.810228605019867E-9),
                PredictedLink.of(4, 0, 0.49750002083312506),
                PredictedLink.of(4, 1, 0.11815697780926958)
            );

        assertThat(predictedLinks)
            .isSubsetOf(expectedLinks)
            .allMatch(prediction -> !graph.exists(prediction.sourceId(), prediction.targetId()));

    }

    @Test
    void shouldPredictTwice() {
        double[] weights = {-2.0, -1.0, 3.0};

        var modelData = ImmutableLinkLogisticRegressionData.of(
            new Weights<>(new Matrix(
                weights,
                1,
                weights.length
            )),
            Weights.ofScalar(0)
        );

        var expectedLinks = List.of(
            PredictedLink.of(0, 4, 0.49750002083312506),
            PredictedLink.of(1, 4, 0.11815697780926958),
            PredictedLink.of(2, 0, 2.0547103309367397E-4),
            PredictedLink.of(3, 0, 0.0024726231566347774),
            PredictedLink.of(4, 0, 0.49750002083312506)
        );

        for (int i = 0; i < 2; i++) {
            var linkPrediction = new ApproximateLinkPrediction(
                modelData,
                LinkFeatureExtractor.of(graph, List.of(new L2FeatureStep(List.of("a", "b", "c")))),
                graph,
                ImmutableKnnBaseConfig.builder()
                    .randomSeed(42L)
                    .concurrency(1)
                    .randomJoins(10)
                    .maxIterations(10)
                    .sampleRate(0.9)
                    .deltaThreshold(0)
                    .topK(1)
                    .nodeWeightProperty("DUMMY")
                    .build(),
                ProgressTracker.NULL_TRACKER
            );

            var predictionResult = linkPrediction.compute();
            var predictedLinks = predictionResult.stream().collect(Collectors.toList());
            assertThat(predictedLinks).hasSize(5);

            assertThat(predictedLinks).containsAll(expectedLinks);
        }
    }

    @Test
    void shouldNotPredictExistingLinks() {
        int topK = 50;
        var pipeline = new LinkPredictionPipeline();
        pipeline.addFeatureStep(new L2FeatureStep(List.of("a", "b", "c")));

        var modelData = ImmutableLinkLogisticRegressionData.of(
            new Weights<>(
                new Matrix(
                    WEIGHTS,
                    1,
                    WEIGHTS.length
                )),
            Weights.ofScalar(0)
        );

        var linkPrediction = new ApproximateLinkPrediction(
            modelData,
            LinkFeatureExtractor.of(graph, List.of(new L2FeatureStep(List.of("a", "b", "c")))),
            graph,
            ImmutableKnnBaseConfig.builder()
                .randomSeed(42L)
                .concurrency(1)
                .randomJoins(10)
                .maxIterations(10)
                .sampleRate(0.9)
                .deltaThreshold(0)
                .topK(topK)
                .nodeWeightProperty("DUMMY")
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
}
