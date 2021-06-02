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
package org.neo4j.gds.estimation.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.gds.embeddings.fastrp.FastRPStatsProc;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamProc;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictMutateProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictStreamProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictWriteProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.singlesource.AllShortestPathsDijkstraMutateProc;
import org.neo4j.gds.paths.singlesource.AllShortestPathsDijkstraStreamProc;
import org.neo4j.gds.paths.singlesource.AllShortestPathsDijkstraWriteProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathAStarMutateProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathAStarStreamProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathAStarWriteProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathDijkstraMutateProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathDijkstraStreamProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathDijkstraWriteProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathYensMutateProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathYensStreamProc;
import org.neo4j.gds.paths.sourcetarget.ShortestPathYensWriteProc;
import org.neo4j.graphalgo.beta.fastrp.FastRPExtendedMutateProc;
import org.neo4j.graphalgo.beta.fastrp.FastRPExtendedStatsProc;
import org.neo4j.graphalgo.beta.fastrp.FastRPExtendedStreamProc;
import org.neo4j.graphalgo.beta.fastrp.FastRPExtendedWriteProc;
import org.neo4j.graphalgo.beta.k1coloring.K1ColoringMutateProc;
import org.neo4j.graphalgo.beta.k1coloring.K1ColoringStatsProc;
import org.neo4j.graphalgo.beta.k1coloring.K1ColoringStreamProc;
import org.neo4j.graphalgo.beta.k1coloring.K1ColoringWriteProc;
import org.neo4j.graphalgo.beta.modularity.ModularityOptimizationMutateProc;
import org.neo4j.graphalgo.beta.modularity.ModularityOptimizationStreamProc;
import org.neo4j.graphalgo.beta.modularity.ModularityOptimizationWriteProc;
import org.neo4j.graphalgo.beta.node2vec.Node2VecMutateProc;
import org.neo4j.graphalgo.beta.node2vec.Node2VecStreamProc;
import org.neo4j.graphalgo.beta.node2vec.Node2VecWriteProc;
import org.neo4j.graphalgo.betweenness.BetweennessCentralityMutateProc;
import org.neo4j.graphalgo.betweenness.BetweennessCentralityStatsProc;
import org.neo4j.graphalgo.betweenness.BetweennessCentralityStreamProc;
import org.neo4j.graphalgo.betweenness.BetweennessCentralityWriteProc;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.config.MutateRelationshipConfig;
import org.neo4j.graphalgo.config.WriteRelationshipConfig;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.degree.DegreeCentralityMutateProc;
import org.neo4j.graphalgo.degree.DegreeCentralityStatsProc;
import org.neo4j.graphalgo.degree.DegreeCentralityStreamProc;
import org.neo4j.graphalgo.degree.DegreeCentralityWriteProc;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionTest;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationMutateProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationStatsProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationStreamProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationWriteProc;
import org.neo4j.graphalgo.louvain.LouvainMutateProc;
import org.neo4j.graphalgo.louvain.LouvainStatsProc;
import org.neo4j.graphalgo.louvain.LouvainStreamProc;
import org.neo4j.graphalgo.louvain.LouvainWriteProc;
import org.neo4j.graphalgo.pagerank.ArticleRankMutateProc;
import org.neo4j.graphalgo.pagerank.ArticleRankStatsProc;
import org.neo4j.graphalgo.pagerank.ArticleRankStreamProc;
import org.neo4j.graphalgo.pagerank.ArticleRankWriteProc;
import org.neo4j.graphalgo.pagerank.EigenvectorMutateProc;
import org.neo4j.graphalgo.pagerank.EigenvectorStatsProc;
import org.neo4j.graphalgo.pagerank.EigenvectorStreamProc;
import org.neo4j.graphalgo.pagerank.EigenvectorWriteProc;
import org.neo4j.graphalgo.pagerank.PageRankMutateProc;
import org.neo4j.graphalgo.pagerank.PageRankStatsProc;
import org.neo4j.graphalgo.pagerank.PageRankStreamProc;
import org.neo4j.graphalgo.pagerank.PageRankWriteProc;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.graphalgo.similarity.knn.KnnMutateProc;
import org.neo4j.graphalgo.similarity.knn.KnnStatsProc;
import org.neo4j.graphalgo.similarity.knn.KnnStreamProc;
import org.neo4j.graphalgo.similarity.knn.KnnWriteProc;
import org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityMutateProc;
import org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityStatsProc;
import org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityStreamProc;
import org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityWriteProc;
import org.neo4j.graphalgo.triangle.LocalClusteringCoefficientMutateProc;
import org.neo4j.graphalgo.triangle.LocalClusteringCoefficientStatsProc;
import org.neo4j.graphalgo.triangle.LocalClusteringCoefficientStreamProc;
import org.neo4j.graphalgo.triangle.LocalClusteringCoefficientWriteProc;
import org.neo4j.graphalgo.triangle.TriangleCountMutateProc;
import org.neo4j.graphalgo.triangle.TriangleCountStatsProc;
import org.neo4j.graphalgo.triangle.TriangleCountStreamProc;
import org.neo4j.graphalgo.triangle.TriangleCountWriteProc;
import org.neo4j.graphalgo.wcc.WccMutateProc;
import org.neo4j.graphalgo.wcc.WccStatsProc;
import org.neo4j.graphalgo.wcc.WccStreamProc;
import org.neo4j.graphalgo.wcc.WccWriteProc;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@GdsEditionTest(value = Edition.EE)
final class EstimationCliTest {

    private static final String PR_ESTIMATE = "gds.pageRank.stream.estimate";

    private static final String EXPECTED_JSON_TEMPLATE =
        "{\n" +
        "  \"bytes_min_resident\" : %d,\n" +
        "  \"bytes_max_resident\" : %d,\n" +
        "  \"min_memory_resident\" : \"%s\",\n" +
        "  \"max_memory_resident\" : \"%s\",\n" +
        "  \"procedure\" : \"%s\",\n" +
        "  \"node_count\" : 42,\n" +
        "  \"relationship_count\" : 1337,\n" +
        "  \"label_count\" : 0,\n" +
        "  \"relationship_type_count\" : 0,\n" +
        "  \"node_property_count\" : 0,\n" +
        "  \"relationship_property_count\" : 0,\n" +
        "  \"peak_memory_factor\" : %.1f,\n" +
        "  \"bytes_min_peak\" : %d,\n" +
        "  \"min_memory_peak\" : \"%s\",\n" +
        "  \"bytes_max_peak\" : %d,\n" +
        "  \"max_memory_peak\" : \"%s\"\n" +
        "}";

    private static final List<String> PROCEDURES = List.of(
        "gds.allShortestPaths.dijkstra.mutate.estimate",
        "gds.allShortestPaths.dijkstra.stream.estimate",
        "gds.allShortestPaths.dijkstra.write.estimate",

        "gds.alpha.ml.nodeClassification.predict.mutate.estimate",
        "gds.alpha.ml.nodeClassification.predict.stream.estimate",
        "gds.alpha.ml.nodeClassification.predict.write.estimate",
        "gds.alpha.ml.nodeClassification.train.estimate",

        "gds.articleRank.mutate.estimate",
        "gds.articleRank.stats.estimate",
        "gds.articleRank.stream.estimate",
        "gds.articleRank.write.estimate",

        "gds.beta.fastRPExtended.mutate.estimate",
        "gds.beta.fastRPExtended.stats.estimate",
        "gds.beta.fastRPExtended.stream.estimate",
        "gds.beta.fastRPExtended.write.estimate",

        "gds.beta.k1coloring.mutate.estimate",
        "gds.beta.k1coloring.stats.estimate",
        "gds.beta.k1coloring.stream.estimate",
        "gds.beta.k1coloring.write.estimate",

        "gds.beta.knn.mutate.estimate",
        "gds.beta.knn.stats.estimate",
        "gds.beta.knn.stream.estimate",
        "gds.beta.knn.write.estimate",

        "gds.beta.modularityOptimization.mutate.estimate",
        "gds.beta.modularityOptimization.stream.estimate",
        "gds.beta.modularityOptimization.write.estimate",

        "gds.beta.node2vec.mutate.estimate",
        "gds.beta.node2vec.stream.estimate",
        "gds.beta.node2vec.write.estimate",

        "gds.betweenness.mutate.estimate",
        "gds.betweenness.stats.estimate",
        "gds.betweenness.stream.estimate",
        "gds.betweenness.write.estimate",

        "gds.degree.mutate.estimate",
        "gds.degree.stats.estimate",
        "gds.degree.stream.estimate",
        "gds.degree.write.estimate",

        "gds.eigenvector.mutate.estimate",
        "gds.eigenvector.stats.estimate",
        "gds.eigenvector.stream.estimate",
        "gds.eigenvector.write.estimate",

        "gds.fastRP.mutate.estimate",
        "gds.fastRP.stats.estimate",
        "gds.fastRP.stream.estimate",
        "gds.fastRP.write.estimate",

        "gds.graph.create.cypher.estimate",
        "gds.graph.create.estimate",

        "gds.labelPropagation.mutate.estimate",
        "gds.labelPropagation.stats.estimate",
        "gds.labelPropagation.stream.estimate",
        "gds.labelPropagation.write.estimate",

        "gds.localClusteringCoefficient.mutate.estimate",
        "gds.localClusteringCoefficient.stats.estimate",
        "gds.localClusteringCoefficient.stream.estimate",
        "gds.localClusteringCoefficient.write.estimate",

        "gds.louvain.mutate.estimate",
        "gds.louvain.stats.estimate",
        "gds.louvain.stream.estimate",
        "gds.louvain.write.estimate",

        "gds.nodeSimilarity.mutate.estimate",
        "gds.nodeSimilarity.stats.estimate",
        "gds.nodeSimilarity.stream.estimate",
        "gds.nodeSimilarity.write.estimate",

        "gds.pageRank.mutate.estimate",
        "gds.pageRank.stats.estimate",
        "gds.pageRank.stream.estimate",
        "gds.pageRank.write.estimate",

        "gds.shortestPath.astar.mutate.estimate",
        "gds.shortestPath.astar.stream.estimate",
        "gds.shortestPath.astar.write.estimate",
        "gds.shortestPath.dijkstra.mutate.estimate",
        "gds.shortestPath.dijkstra.stream.estimate",
        "gds.shortestPath.dijkstra.write.estimate",
        "gds.shortestPath.yens.mutate.estimate",
        "gds.shortestPath.yens.stream.estimate",
        "gds.shortestPath.yens.write.estimate",

        "gds.triangleCount.mutate.estimate",
        "gds.triangleCount.stats.estimate",
        "gds.triangleCount.stream.estimate",
        "gds.triangleCount.write.estimate",

        "gds.wcc.mutate.estimate",
        "gds.wcc.stats.estimate",
        "gds.wcc.stream.estimate",
        "gds.wcc.write.estimate"
    );


    @ParameterizedTest
    @CsvSource({
        "--nodes, --relationships",
        "-n, -r",
    })
    void runsEstimation(String nodeArg, String relArg) {
        var actual = run(PR_ESTIMATE, nodeArg, 42, relArg, 1337);
        var expected = pageRankEstimate();

        assertEquals("gds.pagerank.stream.estimate," + expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @ParameterizedTest
    @CsvSource({
        "K, 1", "M, 2", "G, 3", "T, 4", "P, 5", "E, 6", "Z, 7", "Y, 8",
        "KB, 1", "MB, 2", "GB, 3", "TB, 4", "PB, 5", "EB, 6", "ZB, 7", "YB, 8"
    })
    void canSpecifyBlockSize(String unit, int factor) {
        var nodeCount = 1000_000_000_000L;
        var relationshipCount = 10_000_000_000_000L;
        var actual = run(PR_ESTIMATE, "--nodes", nodeCount, "--relationships", relationshipCount, "--block-size", unit);
        var estimation = pageRankEstimate("nodeCount", nodeCount, "relationshipCount", relationshipCount);
        var scale = Math.pow(
            unit.endsWith("B") ? 1000.0 : 1024.0,
            factor
        );

        var expected = formatWithLocale(
            "gds.pagerank.stream.estimate,%.0f%s,%.0f%s",
            estimation.bytesMin / scale, unit,
            estimation.bytesMax / scale, unit
        );
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @CsvSource({
        "-K, K, 1", "-M, M, 2", "-G, G, 3",
        "-KB, KB, 1", "-MB, MB, 2", "-GB, GB, 3"
    })
    void canSpecifyBlockSizeWithShorthand(String option, String unit, int factor) {
        var nodeCount = 1000_000_000_000L;
        var relationshipCount = 10_000_000_000_000L;
        var actual = run(PR_ESTIMATE, "--nodes", nodeCount, "--relationships", relationshipCount, option);
        var estimation = pageRankEstimate("nodeCount", nodeCount, "relationshipCount", relationshipCount);
        var scale = Math.pow(
            unit.endsWith("B") ? 1000.0 : 1024.0,
            factor
        );

        var expected = formatWithLocale(
            "gds.pagerank.stream.estimate,%.0f%s,%.0f%s",
            estimation.bytesMin / scale, unit,
            estimation.bytesMax / scale, unit
        );
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @CsvSource({
        "-K, -G",
        "-M, -MB",
        "--block-size=G, -K"
    })
    void failsForMultipleBlockSizeOptions(String option1, String option2) {
        var nodeCount = 42;
        var relationshipCount = 1337;
        var actual = assertThrows(ExecutionFailed.class, () -> run(PR_ESTIMATE, "--nodes", nodeCount, "--relationships", relationshipCount, option1, option2));

        assertEquals(2, actual.exitCode);
        assertEquals(
            formatWithLocale(
                "Error: %s, %s are mutually exclusive (specify only one)",
                option1.startsWith("--block-size") ? "--block-size=<blockSize>" : option1,
                option2
            ), actual.stderr.lines().iterator().next()
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"--labels", "-l"})
    void runsEstimationWithLabels(String labelsArg) {
        var actual = run(PR_ESTIMATE, "--nodes", 42, "--relationships", 1337, labelsArg, 21);
        var expected = pageRankEstimate("nodeProjection", listOfIdentifiers(21));

        assertEquals("gds.pagerank.stream.estimate," + expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"--node-properties", "-np"})
    void runsEstimationWithNodeProperties(String nodePropsArg) {
        var actual = run(PR_ESTIMATE, "--nodes", 42, "--relationships", 1337, nodePropsArg, 21);
        var expected = pageRankEstimate("nodeProperties", listOfIdentifiers(21));

        assertEquals("gds.pagerank.stream.estimate," + expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"--relationship-properties", "-rp"})
    void runsEstimationWithRelationshipProperties(String relPropsArg) {
        var actual = run(PR_ESTIMATE, "--nodes", 42, "--relationships", 1337, relPropsArg, 21);
        var expected = pageRankEstimate("relationshipProperties", listOfIdentifiers(21));

        assertEquals("gds.pagerank.stream.estimate," + expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @Test
    void runsEstimationWithConcurrency() {
        var actual = run(PR_ESTIMATE, "--nodes", 42, "--relationships", 1337, "-c", "readConcurrency=21");
        var expected = pageRankEstimate("readConcurrency", 21);

        assertEquals("gds.pagerank.stream.estimate," + expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @Test
    void estimatesGraphCreate() {
        var actual = run("gds.graph.create", "--nodes", 42, "--relationships", 1337);
        var expected = graphCreateEstimate(false);

        assertEquals("gds.graph.create.estimate," + expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @Test
    void estimatesGraphCreateCypher() {
        var actual = run("gds.graph.create.cypher", "--nodes", 42, "--relationships", 1337);
        var expected = graphCreateEstimate(true);

        assertEquals("gds.graph.create.cypher.estimate," + expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @Test
    void printsTree() {
        var actual = run(PR_ESTIMATE, "-n", 42, "-r", 1337, "--tree");
        var expected = pageRankEstimate();

        assertEquals("gds.pagerank.stream.estimate," + expected.treeView.strip(), actual);
    }

    @Test
    void printsJson() {
        var actual = run(PR_ESTIMATE, "-n", 42, "-r", 1337, "--json");
        var expected = pageRankEstimate();
        var expectedJson = expectedJson(expected, "gds.pagerank.stream.estimate");

        assertEquals(expectedJson, actual);
    }

    @Test
    void nodeCountIsMandatory() {
        var actual = assertThrows(ExecutionFailed.class, () -> run(PR_ESTIMATE, "-r", 1337));

        assertEquals(2, actual.exitCode);
        assertEquals(
            "Missing required option: '--nodes=<nodeCount>'",
            actual.stderr.lines().iterator().next()
        );
    }

    @Test
    void relationshipCountIsMandatory() {
        var actual = assertThrows(ExecutionFailed.class, () -> run(PR_ESTIMATE, "-n", 42));

        assertEquals(2, actual.exitCode);
        assertEquals(
            "Missing required option: '--relationships=<relationshipCount>'",
            actual.stderr.lines().iterator().next()
        );
    }

    @Test
    void cannotPrintTreeAndJson() {
        var actual = assertThrows(
            ExecutionFailed.class,
            () -> run(PR_ESTIMATE, "-n", 42, "-r", 1337, "--json", "--tree")
        );

        assertEquals(2, actual.exitCode);
        assertEquals(
            "Error: --tree, --json are mutually exclusive (specify only one)",
            actual.stderr.lines().iterator().next()
        );
    }

    @Test
    void listAllAvailableProcedures() {
        var actual = run("list-available");
        var expected = PROCEDURES.stream().collect(joining(System.lineSeparator()));

        assertEquals(expected, actual);
    }

    @Test
    void estimateAllAvailableProcedures() {
        Stream<MemoryEstimateResult> expectedEstimations = allEstimations();

        var expectedProcedureNames = PROCEDURES.iterator();
        var expected = expectedEstimations
            .map(e -> expectedProcedureNames.next() + "," + e.bytesMin + "," + e.bytesMax)
            .collect(joining(System.lineSeparator()));

        var actual = run("-n", 42, "-r", 1337);
        assertEquals(expected, actual);
    }

    @Test
    void estimateAllAvailableProceduresInTreeMode() {
        Stream<MemoryEstimateResult> expectedEstimations = allEstimations();

        var expectedProcedureNames = PROCEDURES.iterator();
        var expected = expectedEstimations
            .map(e -> expectedProcedureNames.next() + "," + e.treeView)
            .collect(joining(System.lineSeparator()));

        var actual = run("-n", 42, "-r", 1337, "--tree");
        assertEquals(expected.strip(), actual);
    }

    @Test
    void estimateAllAvailableProceduresInTreeInJsonMode() {
        Stream<MemoryEstimateResult> expectedEstimations = allEstimations();

        var expectedProcedureNames = PROCEDURES.iterator();
        var expected = expectedEstimations
            .map(e -> expectedJson(e, expectedProcedureNames.next()))
            .collect(joining(", ", "[ ", " ]"));

        var actual = run("-n", 42, "-r", 1337, "--json");
        assertEquals(expected, actual);
    }

    private String expectedJson(MemoryEstimateResult expected, String procName) {
        double padding = procName.startsWith("gds.graph.create") ? 1.5 : 1.0;
        long paddedMinBytes = (long) (expected.bytesMin * padding);
        long paddedMaxBytes = (long) (expected.bytesMax * padding);
        return formatWithLocale(
            EXPECTED_JSON_TEMPLATE,
            expected.bytesMin,
            expected.bytesMax,
            humanReadable(expected.bytesMin),
            humanReadable(expected.bytesMax),
            procName,
            padding,
            paddedMinBytes,
            humanReadable(paddedMinBytes),
            paddedMaxBytes,
            humanReadable(paddedMaxBytes)
        );
    }

    private static Stream<MemoryEstimateResult> allEstimations() {
        EstimationCli.addModelWithFeatures("", "model", List.of("a", "b"));
        var result = Stream.of(

            runEstimation(new AllShortestPathsDijkstraStreamProc()::streamEstimate, "sourceNode", 0L),
            runEstimation(new AllShortestPathsDijkstraWriteProc()::writeEstimate,
                "sourceNode", 0L,
                WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(new AllShortestPathsDijkstraMutateProc()::mutateEstimate,
                "sourceNode", 0L,
                MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),

            runEstimation(
                new NodeClassificationTrainProc()::estimate,
                "holdoutFraction", 0.2,
                "validationFolds", 5,
                "params", List.of(
                    Map.of("penalty", 0.0625),
                    Map.of("penalty", 0.125)
                ),
                "metrics", List.of("F1_MACRO"),
                "targetProperty", "target",
                "modelName", "model"
            ),
            runEstimation(new NodeClassificationPredictStreamProc()::estimate, "modelName", "model"),
            runEstimation(new NodeClassificationPredictWriteProc()::estimate, "modelName", "model", "writeProperty", "foo"),
            runEstimation(new NodeClassificationPredictMutateProc()::estimate, "modelName", "model", "mutateProperty", "foo"),

            runEstimation(new ArticleRankMutateProc()::estimate, "mutateProperty", "foo"),
            runEstimation(new ArticleRankStatsProc()::estimateStats),
            runEstimation(new ArticleRankStreamProc()::estimate),
            runEstimation(new ArticleRankWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(
                new FastRPExtendedMutateProc()::estimate,
                "mutateProperty",
                "foo",
                "embeddingDimension",
                128,
                "propertyDimension",
                64
            ),
            runEstimation(new FastRPExtendedStatsProc()::estimate, "embeddingDimension", 128, "propertyDimension", 64),
            runEstimation(new FastRPExtendedStreamProc()::estimate, "embeddingDimension", 128, "propertyDimension", 64),
            runEstimation(
                new FastRPExtendedWriteProc()::estimate,
                "writeProperty",
                "foo",
                "embeddingDimension",
                128,
                "propertyDimension",
                64
            ),

            runEstimation(new K1ColoringMutateProc()::mutateEstimate, "mutateProperty", "foo"),
            runEstimation(new K1ColoringStatsProc()::estimate),
            runEstimation(new K1ColoringStreamProc()::estimate),
            runEstimation(new K1ColoringWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(
                new KnnMutateProc()::estimateMutate,
                "nodeWeightProperty",
                "foo",
                "mutateProperty",
                "foo",
                "mutateRelationshipType",
                "bar"
            ),
            runEstimation(
                new KnnStatsProc()::estimateStats,
                "nodeWeightProperty",
                "foo"
            ),
            runEstimation(
                new KnnStreamProc()::estimate,
                "nodeWeightProperty",
                "foo"
            ),
            runEstimation(
                new KnnWriteProc()::estimateWrite,
                "nodeWeightProperty",
                "foo",
                "writeProperty",
                "foo",
                "writeRelationshipType",
                "bar"
            ),

            runEstimation(new ModularityOptimizationMutateProc()::mutateEstimate, "mutateProperty", "foo"),
            runEstimation(new ModularityOptimizationStreamProc()::estimate),
            runEstimation(new ModularityOptimizationWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(new Node2VecMutateProc()::estimate, "mutateProperty", "foo"),
            runEstimation(new Node2VecStreamProc()::estimate),
            runEstimation(new Node2VecWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(new BetweennessCentralityMutateProc()::estimate, "mutateProperty", "foo"),
            runEstimation(new BetweennessCentralityStatsProc()::estimate),
            runEstimation(new BetweennessCentralityStreamProc()::estimate),
            runEstimation(new BetweennessCentralityWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(new DegreeCentralityMutateProc()::estimate, "mutateProperty", "foo"),
            runEstimation(new DegreeCentralityStatsProc()::estimate),
            runEstimation(new DegreeCentralityStreamProc()::estimate),
            runEstimation(new DegreeCentralityWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(new EigenvectorMutateProc()::estimate, "mutateProperty", "foo"),
            runEstimation(new EigenvectorStatsProc()::estimateStats),
            runEstimation(new EigenvectorStreamProc()::estimate),
            runEstimation(new EigenvectorWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(new FastRPMutateProc()::estimate, "mutateProperty", "foo", "embeddingDimension", 128),
            runEstimation(new FastRPStatsProc()::estimate, "embeddingDimension", 128),
            runEstimation(new FastRPStreamProc()::estimate, "embeddingDimension", 128),
            runEstimation(new FastRPWriteProc()::estimate, "writeProperty", "foo", "embeddingDimension", 128),


            graphCreateEstimate(false),
            graphCreateEstimate(true),

            runEstimation(new LabelPropagationMutateProc()::mutateEstimate, "mutateProperty", "foo"),
            runEstimation(new LabelPropagationStatsProc()::estimateStats),
            runEstimation(new LabelPropagationStreamProc()::estimate),
            runEstimation(new LabelPropagationWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(new LocalClusteringCoefficientMutateProc()::estimate, "mutateProperty", "foo"),
            runEstimation(new LocalClusteringCoefficientStatsProc()::estimateStats),
            runEstimation(new LocalClusteringCoefficientStreamProc()::estimateStats),
            runEstimation(new LocalClusteringCoefficientWriteProc()::estimateStats, "writeProperty", "foo"),

            runEstimation(new LouvainMutateProc()::estimate, "mutateProperty", "foo"),
            runEstimation(new LouvainStatsProc()::estimateStats),
            runEstimation(new LouvainStreamProc()::estimate),
            runEstimation(new LouvainWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(
                new NodeSimilarityMutateProc()::estimateMutate,
                "mutateProperty",
                "foo",
                "mutateRelationshipType",
                "bar"
            ),
            runEstimation(new NodeSimilarityStatsProc()::estimateStats),
            runEstimation(new NodeSimilarityStreamProc()::estimate),
            runEstimation(
                new NodeSimilarityWriteProc()::estimateWrite,
                "writeProperty",
                "foo",
                "writeRelationshipType",
                "bar"
            ),

            runEstimation(new PageRankMutateProc()::estimate, "mutateProperty", "foo"),
            runEstimation(new PageRankStatsProc()::estimateStats),
            runEstimation(new PageRankStreamProc()::estimate),
            runEstimation(new PageRankWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(new ShortestPathAStarStreamProc()::streamEstimate,
                "sourceNode", 0L,
                "targetNode", 1L,
                ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY, "LAT",
                ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY, "LON"
            ),
            runEstimation(new ShortestPathAStarWriteProc()::writeEstimate,
                "sourceNode", 0L,
                "targetNode", 1L,
                ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY, "LAT",
                ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY, "LON",
                WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(new ShortestPathAStarMutateProc()::mutateEstimate,
                "sourceNode", 0L,
                "targetNode", 1L,
                ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY, "LAT",
                ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY, "LON",
                MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),

            runEstimation(new ShortestPathDijkstraStreamProc()::streamEstimate, "sourceNode", 0L, "targetNode", 1L),
            runEstimation(new ShortestPathDijkstraWriteProc()::writeEstimate,
                "sourceNode", 0L,
                "targetNode", 1L,
                WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(new ShortestPathDijkstraMutateProc()::mutateEstimate,
                "sourceNode", 0L,
                "targetNode", 1L,
                MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(new ShortestPathYensStreamProc()::streamEstimate, "sourceNode", 0L, "targetNode", 1L, "k", 3),
            runEstimation(new ShortestPathYensWriteProc()::writeEstimate,
                "sourceNode", 0L,
                "targetNode", 1L,
                "k", 3,
                WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(new ShortestPathYensMutateProc()::mutateEstimate,
                "sourceNode", 0L,
                "targetNode", 1L,
                "k", 3,
                MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),

            runEstimation(new TriangleCountMutateProc()::estimate, "mutateProperty", "foo"),
            runEstimation(new TriangleCountStatsProc()::estimateStats),
            runEstimation(new TriangleCountStreamProc()::estimateStats),
            runEstimation(new TriangleCountWriteProc()::estimate, "writeProperty", "foo"),

            runEstimation(new WccMutateProc()::mutateEstimate, "mutateProperty", "foo"),
            runEstimation(new WccStatsProc()::statsEstimate),
            runEstimation(new WccStreamProc()::streamEstimate),
            runEstimation(new WccWriteProc()::writeEstimate, "writeProperty", "foo")
        );
        ModelCatalog.removeAllLoadedModels();
        return result;
    }

    private static final class ExecutionFailed extends RuntimeException {
        final int exitCode;
        final String stderr;

        ExecutionFailed(int exitCode, String stderr) {
            super(formatWithLocale(
                "Calling CLI failed with exit code %d and stderr: %s",
                exitCode,
                stderr
            ));
            this.exitCode = exitCode;
            this.stderr = stderr;
        }
    }

    private static String run(Object... args) {
        var arguments = new String[args.length];
        Arrays.setAll(arguments, i -> String.valueOf(args[i]));

        var stdout = new ByteArrayOutputStream(8192);
        var stderr = new ByteArrayOutputStream(8192);
        var originalOut = System.out;
        var originalErr = System.err;
        var exitCode = -1;

        try {

            System.setOut(new PrintStream(stdout, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));

            exitCode = EstimationCli.runWithArgs(arguments);

        } finally {
            System.setErr(originalErr);
            System.setOut(originalOut);
        }


        if (exitCode != 0) {
            throw new ExecutionFailed(exitCode, stderr.toString(StandardCharsets.UTF_8));
        }

        return stdout.toString(StandardCharsets.UTF_8).strip();
    }

    private static MemoryEstimateResult pageRankEstimate(Object... config) {
        return runEstimation(new PageRankStreamProc()::estimate, config);
    }

    private static MemoryEstimateResult runEstimation(
        BiFunction<Object, Map<String, Object>, Stream<MemoryEstimateResult>> proc,
        Object... config
    ) {
        Map<String, Object> configMap = new HashMap<>(Map.of(
            "nodeCount", 42L,
            "relationshipCount", 1337L,
            "nodeProjection", "*",
            "relationshipProjection", "*"
        ));
        for (int i = 0; i < config.length; i += 2) {
            configMap.put(String.valueOf(config[i]), config[i + 1]);
        }
        return proc.apply(configMap, Map.of()).iterator().next();
    }

    private static MemoryEstimateResult graphCreateEstimate(boolean cypher) {
        Map<String, Object> config = Map.of(
            "nodeCount", 42L,
            "relationshipCount", 1337L
        );

        var gc = new GraphCreateProc();
        var result = cypher
            ? gc.createCypherEstimate(ALL_NODES_QUERY, ALL_RELATIONSHIPS_QUERY, config)
            : gc.createEstimate("*", "*", config);

        return result.iterator().next();
    }

    private static List<String> listOfIdentifiers(int numberOfIdentifiers) {
        return IntStream
            .range(0, numberOfIdentifiers)
            .mapToObj(i -> String.valueOf((char) ('A' + i)))
            .collect(toList());
    }

}
