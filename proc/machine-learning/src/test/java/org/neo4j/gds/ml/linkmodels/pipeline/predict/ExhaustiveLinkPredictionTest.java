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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.PredictedLink;
import org.neo4j.gds.ml.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertMemoryRange;
import static org.neo4j.gds.ml.linkmodels.pipeline.predict.ApproximateLinkPredictionTest.compareWithPrecision;

@GdlExtension
class ExhaustiveLinkPredictionTest extends BaseProcTest {
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

    private GraphStore graphStore;

    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "multiLabel")
    static String gdlMultiLabel = "(n0 :A {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n1: B {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n2: C {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n3: B {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n4: C {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n5: A {a: 1.0, b: 0.8, c: 1.0})" +
                                  "(n0)-[:T]->(n1), (n1)-[:T]->(n2), (n2)-[:T}->(n0), (n5)-[:T]->(n1)";


    @Inject
    TestGraph multiLabelGraph;

    @Inject
    GraphStore multiLabelGraphStore;

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
    }

    @ParameterizedTest
    @CsvSource(value = {"3, 1", "3, 4", "50, 1", "50, 4"})
    void shouldPredictWithTopN(int topN, int concurrency) {
        var featureStep = new L2FeatureStep(List.of("a", "b", "c"));

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

        var graph = graphStore.getGraph(
            List.of(NodeLabel.of("N")),
            List.of(RelationshipType.of("T")),
            Optional.empty()
        );
        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, List.of(featureStep));
        var linkPrediction = new ExhaustiveLinkPrediction(
            LogisticRegressionClassifier.from(modelData),
            linkFeatureExtractor,
            graph,
            "N",
            "N",
            concurrency,
            topN,
            0D,
            ProgressTracker.NULL_TRACKER
        );

        var predictionResult = linkPrediction.compute();
        assertThat(predictionResult.samplingStats()).isEqualTo(
            Map.of(
                "strategy", "exhaustive",
                "linksConsidered", 6L
            )
        );


        var predictedLinks = predictionResult.stream().collect(Collectors.toList());
        assertThat(predictedLinks).hasSize(Math.min(topN, 6));

        var expectedLinks = List.of(
            PredictedLink.of(0, 4, 0.49750002083312506),
            PredictedLink.of(1, 4, 0.11815697780926955),
            PredictedLink.of(0, 1, 0.1150667320455498),
            PredictedLink.of(0, 3, 0.0024726231566347765),
            PredictedLink.of(0, 2, 2.054710330936739E-4),
            PredictedLink.of(2, 3, 2.810228605019864E-9)
        );

        var endIndex = Math.min(topN, expectedLinks.size());
        assertThat(predictedLinks)
            .usingElementComparator(compareWithPrecision(1e-10))
            .containsExactly(expectedLinks
            .subList(0, endIndex)
            .toArray(PredictedLink[]::new));
    }

    @ParameterizedTest
    @CsvSource(value = {"1, 0.3", "3, 0.05", "4, 0.002", "6, 0.00000000001", "6, 0.0"})
    void shouldPredictWithThreshold(int expectedPredictions, double threshold) {
        var featureStep = new L2FeatureStep(List.of("a", "b", "c"));

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

        var graph = graphStore.getGraph(
            List.of(NodeLabel.of("N")),
            List.of(RelationshipType.of("T")),
            Optional.empty()
        );

        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, List.of(featureStep));

        var linkPrediction = new ExhaustiveLinkPrediction(
            LogisticRegressionClassifier.from(modelData),
            linkFeatureExtractor,
            graph,
            "N",
            "N",
            4,
            6,
            threshold,
            ProgressTracker.NULL_TRACKER
        );
        var predictionResult = linkPrediction.compute();
        var predictedLinks = predictionResult.stream().collect(Collectors.toList());
        assertThat(predictedLinks).hasSize(expectedPredictions);

        assertThat(predictedLinks).allMatch(l -> l.probability() >= threshold);
    }

    //TODO Test filtered predictions on multilabel graphs
    @ParameterizedTest
    @CsvSource(value = {"1", "5"})
    void shouldOnlyPredictOverValidNodeLabels(int topN) {
        var featureStep = new L2FeatureStep(List.of("a", "b", "c"));

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

        var graph = multiLabelGraphStore.getGraph(
            List.of(NodeLabel.of("A"), NodeLabel.of("B"), NodeLabel.of("C")),
            List.of(RelationshipType.of("T")),
            Optional.empty()
        );
        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, List.of(featureStep));
        var linkPrediction = new ExhaustiveLinkPrediction(
            LogisticRegressionClassifier.from(modelData),
            linkFeatureExtractor,
            multiLabelGraph,
            "A",
            "B",
            4,
            topN,
            0D,
            ProgressTracker.NULL_TRACKER
        );

        var predictionResult = linkPrediction.compute();
        assertThat(predictionResult.samplingStats()).isEqualTo(
            Map.of(
                "strategy", "exhaustive",
                //Only (n0)--(n3), (n5)--(n3) respect labels
                "linksConsidered", 2L
            )
        );


        var predictedLinks = predictionResult.stream().collect(Collectors.toList());
        assertThat(predictedLinks).hasSize(Math.min(topN, 2));

        var expectedLinks = List.of(
            PredictedLink.of(0, 3, 0.5),
            PredictedLink.of(3,5,0.5)
        );

        var endIndex = Math.min(topN, expectedLinks.size());
        assertThat(predictedLinks)
            .usingElementComparator(compareWithPrecision(1e-10))
            .containsExactlyInAnyOrder(expectedLinks
                .subList(0, endIndex)
                .toArray(PredictedLink[]::new));
    }

    @ParameterizedTest
    @CsvSource(value = {
        "1, 3_636",
        "10, 3_852"
    })
    void estimateWithDifferentTopN(int topN, long expectedEstimation) {
        var config = LinkPredictionPredictPipelineBaseConfigImpl.builder()
            .topN(topN)
            .username("DUMMY")
            .modelName("DUMMY")
            .graphName("DUMMY")
            .sourceNodeLabel("DUMMY")
            .targetNodeLabel("DUMMY")
            .build();

        var actualEstimate = ExhaustiveLinkPrediction
            .estimate(config, 100)
            .estimate(GraphDimensions.of(100, 1000), config.concurrency());

        assertMemoryRange(actualEstimate.memoryUsage(), expectedEstimation);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "10, 972",
        "1000, 32_652"
    })
    void estimateWithDifferentLinkFeatureDimension(int linkFeatureDimension, long expectedEstimation) {
        var config = LinkPredictionPredictPipelineBaseConfigImpl.builder()
            .topN(10)
            .username("DUMMY")
            .modelName("DUMMY")
            .graphName("DUMMY")
            .sourceNodeLabel("DUMMY")
            .targetNodeLabel("DUMMY")
            .build();

        var actualEstimate = ExhaustiveLinkPrediction
            .estimate(config, linkFeatureDimension)
            .estimate(GraphDimensions.of(100, 1000), config.concurrency());

        assertMemoryRange(actualEstimate.memoryUsage(), expectedEstimation);
    }

}
