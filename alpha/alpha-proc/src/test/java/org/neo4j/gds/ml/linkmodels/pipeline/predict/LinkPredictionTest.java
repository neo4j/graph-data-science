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
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
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
import org.neo4j.gds.ml.linkmodels.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.linkmodels.pipeline.TrainingPipeline;
import org.neo4j.gds.ml.linkmodels.pipeline.ProcedureTestUtils;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.L2FeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.ImmutableLinkLogisticRegressionData;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class LinkPredictionTest extends BaseProcTest {
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

        graph = GraphStoreCatalog.get(getUsername(), db.databaseId(), "g").graphStore().getUnion();
    }

    @ParameterizedTest
    @CsvSource(value = {"3, 1", "3, 4", "50, 1", "50, 4"})
    void shouldPredictWithTopN(int topN, int concurrency) {
        var pipeline = new TrainingPipeline();
        pipeline.addFeatureStep(new L2FeatureStep(List.of("a", "b", "c")));

        var modelData = ImmutableLinkLogisticRegressionData.of(new Weights<>(new Matrix(
            WEIGHTS,
            1,
            WEIGHTS.length
        )));

        ProcedureTestUtils.applyOnProcedure(db, (Consumer<? super AlgoBaseProc<?, ?, ?>>) caller -> {
            var linkPrediction = new LinkPrediction(
                modelData,
                new PipelineExecutor(pipeline, caller, db.databaseId(), getUsername(), GRAPH_NAME),
                List.of(NodeLabel.of("N")),
                List.of(RelationshipType.of("T")),
                graph,
                concurrency,
                topN,
                0D,
                ProgressTracker.NULL_TRACKER
            );
            var predictionResult = linkPrediction.compute();
            var predictedLinks = predictionResult.stream().collect(Collectors.toList());
            assertThat(predictedLinks).hasSize(Math.min(topN, 6));

            var expectedLinks = List.of(
                PredictedLink.of(0, 4, 0.49750002083312506),
                PredictedLink.of(1, 4, 0.11815697780926958),
                PredictedLink.of(0, 1, 0.11506673204554983),
                PredictedLink.of(0, 3, 0.0024726231566347774),
                PredictedLink.of(0, 2, 2.0547103309367397E-4),
                PredictedLink.of(2, 3, 2.810228605019867E-9)
            );
            assertThat(expectedLinks).containsAll(predictedLinks);
        });
    }

    @ParameterizedTest
    @CsvSource(value = {"1, 0.3", "3, 0.05", "4, 0.002", "6, 0.00000000001", "6, 0.0"})
    void shouldPredictWithThreshold(int expectedPredictions, double threshold) {
        var pipeline = new TrainingPipeline();
        pipeline.addFeatureStep(new L2FeatureStep(List.of("a", "b", "c")));

        var modelData = ImmutableLinkLogisticRegressionData.of(new Weights<>(new Matrix(
            WEIGHTS,
            1,
            WEIGHTS.length
        )));

        ProcedureTestUtils.applyOnProcedure(db, (Consumer<? super AlgoBaseProc<?, ?, ?>>) caller -> {
            var linkPrediction = new LinkPrediction(
                modelData,
                new PipelineExecutor(pipeline, caller, db.databaseId(), getUsername(), GRAPH_NAME),
                List.of(NodeLabel.of("N")),
                List.of(RelationshipType.of("T")),
                graph,
                4,
                6,
                threshold,
                ProgressTracker.NULL_TRACKER
            );
            var predictionResult = linkPrediction.compute();
            var predictedLinks = predictionResult.stream().collect(Collectors.toList());
            assertThat(predictedLinks).hasSize(expectedPredictions);

            assertThat(predictedLinks).allMatch(l -> l.probability() >= threshold);
        });
    }
}
