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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPredictPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfigImpl;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline.MODEL_TYPE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Neo4jModelCatalogExtension
class LinkPredictionPipelineMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    static String DB_QUERY =
        "CREATE " +
        "  (n0:N {a: 1.0, b: 0.8, c: 1.0})" +
        ", (n1:N {a: 2.0, b: 1.0, c: 1.0})" +
        ", (n2:N {a: 3.0, b: 1.5, c: 1.0})" +
        ", (n3:N {a: 0.0, b: 2.8, c: 1.0})" +
        ", (n4:N {a: 1.0, b: 0.9, c: 1.0})" +
        ", (m0:M {a: 1.0, b: 0.8, c: 1.0})" +
        ", (m1:M {a: 2.0, b: 1.0, c: 1.0})" +
        ", (m2:M {a: 3.0, b: 1.5, c: 1.0})" +
        ", (m3:M {a: 0.0, b: 2.8, c: 1.0})" +
        ", (m4:M {a: 1.0, b: 0.9, c: 1.0})" +
        ", (n1)-[:T]->(n2)" +
        ", (n3)-[:T]->(n4)" +
        ", (n1)-[:T]->(n3)" +
        ", (n2)-[:T]->(n4)" +
        ", (m1)-[:T]->(m2)" +
        ", (m3)-[:T]->(m4)" +
        ", (m1)-[:T]->(m3)" +
        ", (m2)-[:T]->(m4)";

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphListProc.class,
            GraphProjectProc.class,
            LinkPredictionPipelineMutateProc.class
        );

        withModelInCatalog();
        var query = "CALL gds.graph.project(" +
                    "'g', " +
                    "{" +
                    "  M: { label: 'M', properties: ['a', 'b', 'c'] }, " +
                    "  N: { label: 'N', properties: ['a', 'b', 'c'] }" +
                    "}, " +
                    "{T: {type: 'T', orientation: 'UNDIRECTED'}})";
        runQuery(query);
    }

    @ParameterizedTest
    @MethodSource("topNConcurrencyLabelCombinations")
    void shouldPredictWithTopN(int topN, int concurrency, String nodeLabel) {
        runQuery(
            "CALL gds.beta.pipeline.linkPrediction.predict.mutate('g', {" +
            " modelName: 'model'," +
            " sourceNodeLabel: $sourceNodeLabel," +
            " targetNodeLabel: $targetNodeLabel," +
            " mutateRelationshipType: 'PREDICTED'," +
            " threshold: 0," +
            " topN: $topN," +
            " concurrency:" +
            " $concurrency" +
            "})",
            Map.of(
                "sourceNodeLabel", nodeLabel,
                "targetNodeLabel", nodeLabel,
                "topN", topN,
                "concurrency", concurrency,
                "nodeLabel", nodeLabel
            )
        );

        Graph actualGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "g").graphStore().getGraph(
            NodeLabel.of(nodeLabel), RelationshipType.of("PREDICTED"), Optional.of("probability"));

        var relationshipRowsGdl = List.of(
            "(n0)-[:PREDICTED {probability: 0.49750002083312506}]->(n4)",
            "(n4)-[:PREDICTED {probability: 0.49750002083312506}]->(n0)",
            "(n1)-[:PREDICTED {probability: 0.11815697780926958}]->(n4)",
            "(n4)-[:PREDICTED {probability: 0.11815697780926958}]->(n1)",
            "(n0)-[:PREDICTED {probability: 0.11506673204554983}]->(n1)",
            "(n1)-[:PREDICTED {probability: 0.11506673204554983}]->(n0)",
            "(n0)-[:PREDICTED {probability: 0.0024726231566347774}]->(n3)",
            "(n3)-[:PREDICTED {probability: 0.0024726231566347774}]->(n0)",
            "(n0)-[:PREDICTED {probability: 0.00020547103309367397}]->(n2)",
            "(n2)-[:PREDICTED {probability: 0.00020547103309367397}]->(n0)",
            "(n2)-[:PREDICTED {probability: 0.000000002810228605019867}]->(n3)",
            "(n3)-[:PREDICTED {probability: 0.000000002810228605019867}]->(n2)"
           // 2 * because result graph is undirected
        ).subList(0, 2 * Math.min(topN, 6));
        var relationshipGdl = String.join(",", relationshipRowsGdl);

        assertGraphEquals(
            fromGdl(
                formatWithLocale(
                    "  (n0:%1$s {a: 1.0, b: 0.8, c: 1.0})" +
                    ", (n1:%1$s {a: 2.0, b: 1.0, c: 1.0})" +
                    ", (n2:%1$s {a: 3.0, b: 1.5, c: 1.0})" +
                    ", (n3:%1$s {a: 0.0, b: 2.8, c: 1.0})" +
                    ", (n4:%1$s {a: 1.0, b: 0.9, c: 1.0})" + relationshipGdl,
                    nodeLabel)
            ), actualGraph);
    }

    @Test
    void checkYieldsAndMutatedTypeAndProperty() {
        var graphStore = GraphStoreCatalog
            .get(getUsername(), DatabaseId.of(db), "g")
            .graphStore();

        var query = GdsCypher
            .call("g")
            .algo("gds.beta.pipeline.linkPrediction.predict")
            .mutateMode()
            .addParameter("mutateRelationshipType", "PREDICTED")
            .addParameter("modelName", "model")
            .addParameter("threshold", 0.0)
            .addParameter("topN", 6)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "postProcessingMillis", 0L,
            // we are writing undirected rels so we get 2x topN
            "relationshipsWritten", 12L,
            "configuration", isA(Map.class),
            "probabilityDistribution", allOf(
                hasKey("min"),
                hasKey("max"),
                hasKey("mean"),
                hasKey("stdDev"),
                hasKey("p1"),
                hasKey("p5"),
                hasKey("p10"),
                hasKey("p25"),
                hasKey("p50"),
                hasKey("p75"),
                hasKey("p90"),
                hasKey("p95"),
                hasKey("p99"),
                hasKey("p100")
            ),
            "samplingStats", Map.of(
                "strategy", "exhaustive",
                "linksConsidered", 6L
            )
        )));

        assertTrue(graphStore.hasRelationshipProperty(RelationshipType.of("PREDICTED"), "probability"));
    }

    private void withModelInCatalog() {
        var weights = new double[]{2.0, 1.0, -3.0};
        var pipeline = LinkPredictionPredictPipeline.from(Stream.of(), Stream.of(new L2FeatureStep(List.of("a", "b", "c"))));

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(new Matrix(
                weights,
                1,
                weights.length
            )),
            Weights.ofVector(0.0)
        );

        modelCatalog.set(Model.of(
            MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("model")
                .pipeline("DUMMY")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .targetRelationshipType("T")
                .graphName("g")
                .negativeClassWeight(1.0)
                .build(),
            LinkPredictionModelInfo.of(
                Map.of(),
                Map.of(),
                ModelCandidateStats.of(LogisticRegressionTrainConfig.DEFAULT, Map.of(), Map.of()),
                pipeline
            )
        ));
    }

    private static Stream<Arguments> topNConcurrencyLabelCombinations() {
        return crossArguments(
            () -> Stream.of(3, 50).map(Arguments::of),
            () -> Stream.of(1, 4).map(Arguments::of),
            () -> Stream.of("N", "M").map(Arguments::of)
        );
    }
}
