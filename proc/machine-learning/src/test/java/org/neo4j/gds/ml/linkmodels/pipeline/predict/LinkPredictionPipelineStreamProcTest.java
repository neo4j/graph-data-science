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
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.catalog.GraphProjectProc;
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
import java.util.stream.Stream;

import static org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline.MODEL_TYPE;

@Neo4jModelCatalogExtension
class LinkPredictionPipelineStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    static String DB_CYPHER =
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
        registerProcedures(GraphListProc.class, GraphProjectProc.class, LinkPredictionPipelineStreamProc.class);

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
    @CsvSource(value = {"1, N", "4, M"})
    void shouldPredictWithTopN(int concurrency, String nodeLabel) {
        var nodeCount = 5L;
        assertCypherResult("CALL gds.graph.list('g') YIELD nodeCount RETURN nodeCount",
            List.of(Map.of("nodeCount", 2 * nodeCount))
        );
        var labelOffset = nodeLabel.equals("N") ? 0 : nodeCount;
        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.predict.stream('g', {" +
            " modelName: 'model'," +
            " sourceNodeLabel: $nodeLabel," +
            " targetNodeLabel: $nodeLabel," +
            " threshold: 0," +
            " topN: $topN," +
            " concurrency: $concurrency" +
            "})" +
            "YIELD node1, node2, probability" +
            " RETURN node1, node2, probability" +
            " ORDER BY probability DESC, node1",
            Map.of("nodeLabel", nodeLabel, "topN", 3, "concurrency", concurrency),
            List.of(
                Map.of("node1", 0L + labelOffset, "node2", 4L + labelOffset, "probability", .49750002083312506),
                Map.of("node1", 1L + labelOffset, "node2", 4L + labelOffset, "probability", .11815697780926959),
                Map.of("node1", 0L + labelOffset, "node2", 1L + labelOffset, "probability", .11506673204554985)
            )
        );
    }

    @ParameterizedTest
    @CsvSource(value = {"N, [2320 Bytes ... 3664 Bytes]", "M, [2880 Bytes ... 5344 Bytes]"})
    void estimate(String targetNodeLabel, String expectedMemoryRange) {
        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.predict.stream.estimate('g', {" +
            " modelName: 'model'," +
            " sampleRate: 0.5," +
            " targetNodeLabel: $targetNodeLabel," +
            " topK: $topK" +
            "})" +
            "YIELD requiredMemory",
            Map.of("targetNodeLabel", targetNodeLabel, "topK", 3),
            List.of(
                Map.of("requiredMemory", expectedMemoryRange)
            )
        );
    }

    @Test
    void shouldPredictWithInitialSamplerSet() {
        var nodeCount = 5L;
        assertCypherResult("CALL gds.graph.list('g') YIELD nodeCount RETURN nodeCount",
            List.of(Map.of("nodeCount", 2 * nodeCount))
        );
        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.predict.stream('g', {" +
            " modelName: 'model'," +
            " sampleRate: 0.5," +
            " randomSeed: 42," +
            " topK: $topK," +
            " initialSampler: 'randomWalk'," +
            " concurrency: $concurrency" +
            "})" +
            "YIELD node1, node2, probability" +
            " RETURN node1, node2, probability" +
            " ORDER BY probability DESC, node1",
            Map.of("topK", 1, "concurrency", 1),
            List.of(
                Map.of("node1", 0L, "node2", 4L, "probability", .49750002083312506),
                Map.of("node1", 4L, "node2", 0L, "probability", .49750002083312506),
                Map.of("node1", 1L, "node2", 4L, "probability", .11815697780926959),
                Map.of("node1", 3L, "node2", 0L, "probability", .002472623156634657),
                Map.of("node1", 2L, "node2", 0L, "probability", 2.0547103309365156E-4)
            )
        );
    }

    @Test
    void estimateWithFictitiousGraph() {
        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.predict.stream.estimate(" +
            "{ nodeCount: $nodeCount," +
            " relationshipCount: $relationshipCount," +
            " nodeProjection: $sourceNodeLabel," +
            " relationshipProjection: '*'}," +
            "{" +
            " modelName: 'model'," +
            " sourceNodeLabel: $sourceNodeLabel," +
            " targetNodeLabel: $targetNodeLabel," +
            " threshold: 0," +
            " topN: $topN" +
            "})" +
            "YIELD requiredMemory",
            Map.of("nodeCount", 42L, "relationshipCount", 28L, "sourceNodeLabel", "N", "targetNodeLabel", "N", "topN", 3),
            List.of(
                Map.of("requiredMemory", "289 KiB")
            )
        );
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
}
