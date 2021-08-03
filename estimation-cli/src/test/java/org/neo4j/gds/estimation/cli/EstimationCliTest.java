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

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.beta.fastrp.FastRPExtendedMutateProc;
import org.neo4j.gds.beta.fastrp.FastRPExtendedStatsProc;
import org.neo4j.gds.beta.fastrp.FastRPExtendedStreamProc;
import org.neo4j.gds.beta.fastrp.FastRPExtendedWriteProc;
import org.neo4j.gds.beta.k1coloring.K1ColoringMutateProc;
import org.neo4j.gds.beta.k1coloring.K1ColoringStatsProc;
import org.neo4j.gds.beta.k1coloring.K1ColoringStreamProc;
import org.neo4j.gds.beta.k1coloring.K1ColoringWriteProc;
import org.neo4j.gds.beta.modularity.ModularityOptimizationMutateProc;
import org.neo4j.gds.beta.modularity.ModularityOptimizationStreamProc;
import org.neo4j.gds.beta.modularity.ModularityOptimizationWriteProc;
import org.neo4j.gds.beta.node2vec.Node2VecMutateProc;
import org.neo4j.gds.beta.node2vec.Node2VecStreamProc;
import org.neo4j.gds.beta.node2vec.Node2VecWriteProc;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateProc;
import org.neo4j.gds.betweenness.BetweennessCentralityStatsProc;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamProc;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteProc;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.degree.DegreeCentralityMutateProc;
import org.neo4j.gds.degree.DegreeCentralityStatsProc;
import org.neo4j.gds.degree.DegreeCentralityStreamProc;
import org.neo4j.gds.degree.DegreeCentralityWriteProc;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.gds.embeddings.fastrp.FastRPStatsProc;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamProc;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteProc;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.gds.labelpropagation.LabelPropagationMutateProc;
import org.neo4j.gds.labelpropagation.LabelPropagationStatsProc;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamProc;
import org.neo4j.gds.labelpropagation.LabelPropagationWriteProc;
import org.neo4j.gds.louvain.LouvainMutateProc;
import org.neo4j.gds.louvain.LouvainStatsProc;
import org.neo4j.gds.louvain.LouvainStreamProc;
import org.neo4j.gds.louvain.LouvainWriteProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictMutateProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictStreamProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictWriteProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;
import org.neo4j.gds.pagerank.ArticleRankMutateProc;
import org.neo4j.gds.pagerank.ArticleRankStatsProc;
import org.neo4j.gds.pagerank.ArticleRankStreamProc;
import org.neo4j.gds.pagerank.ArticleRankWriteProc;
import org.neo4j.gds.pagerank.EigenvectorMutateProc;
import org.neo4j.gds.pagerank.EigenvectorStatsProc;
import org.neo4j.gds.pagerank.EigenvectorStreamProc;
import org.neo4j.gds.pagerank.EigenvectorWriteProc;
import org.neo4j.gds.pagerank.PageRankMutateProc;
import org.neo4j.gds.pagerank.PageRankStatsProc;
import org.neo4j.gds.pagerank.PageRankStreamProc;
import org.neo4j.gds.pagerank.PageRankWriteProc;
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
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.knn.KnnMutateProc;
import org.neo4j.gds.similarity.knn.KnnStatsProc;
import org.neo4j.gds.similarity.knn.KnnStreamProc;
import org.neo4j.gds.similarity.knn.KnnWriteProc;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateProc;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStatsProc;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStreamProc;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityWriteProc;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMutateProc;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStatsProc;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStreamProc;
import org.neo4j.gds.triangle.LocalClusteringCoefficientWriteProc;
import org.neo4j.gds.triangle.TriangleCountMutateProc;
import org.neo4j.gds.triangle.TriangleCountStatsProc;
import org.neo4j.gds.triangle.TriangleCountStreamProc;
import org.neo4j.gds.triangle.TriangleCountWriteProc;
import org.neo4j.gds.wcc.WccMutateProc;
import org.neo4j.gds.wcc.WccStatsProc;
import org.neo4j.gds.wcc.WccStreamProc;
import org.neo4j.gds.wcc.WccWriteProc;
import org.neo4j.graphalgo.config.MutateRelationshipConfig;
import org.neo4j.graphalgo.config.WriteRelationshipConfig;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.procedure.Procedure;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;

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
        var actual = assertThrows(
            ExecutionFailed.class,
            () -> run(PR_ESTIMATE, "--nodes", nodeCount, "--relationships", relationshipCount, option1, option2)
        );

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

    @ParameterizedTest(name = "{1}")
    @MethodSource("categoryInputs")
    void estimatesCategory(
        Stream<Pair<String, MemoryEstimateResult>> expectedEstimations,
        List<String> categories
    ) {
        var expected = expectedEstimations
            .map(t -> expectedJson(t.getTwo(), t.getOne()))
            .collect(joining(", ", "[ ", " ]"));

        var args = Stream.<Object>concat(
            Stream.of("--nodes", 42, "--relationships", 1337, "--json"),
            categories.stream().flatMap(category -> Stream.of("--category", category))
        );

        var actual = run(args.toArray());
        assertEquals(expected, actual);
    }

    static Stream<Arguments> categoryInputs() {
        return Stream.of(
            arguments(
                communityDetectionEstimations(),
                List.of("community-detection")
            ),
            arguments(
                centralityEstimations(),
                List.of("centrality")
            ),
            arguments(
                similarityEstimations(),
                List.of("similarity")
            ),
            arguments(
                pathFindingEstimations(),
                List.of("path-finding")
            ),
            arguments(
                nodeEmbeddingEstimations(),
                List.of("node-embedding")
            ),
            arguments(
                allEstimations(),
                List.of("machine-learning")
            ),
            arguments(
                sorted(communityDetectionEstimations(), centralityEstimations()),
                List.of("community-detection", "centrality")
            ),
            arguments(
                allEstimations(),
                List.of("community-detection", "machine-learning", "path-finding")
            )
        );
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
        var expected = allEstimations().map(Pair::getOne).collect(joining(System.lineSeparator()));

        assertEquals(expected, actual);
    }

    @Test
    void estimateAllAvailableProcedures() {
        var expectedEstimations = allEstimations();
        var expected = expectedEstimations
            .map(e -> e.getOne() + "," + e.getTwo().bytesMin + "," + e.getTwo().bytesMax)
            .collect(joining(System.lineSeparator()));

        var actual = run("-n", 42, "-r", 1337);
        assertEquals(expected, actual);
    }

    @Test
    void estimateAllAvailableProceduresInTreeMode() {
        var expectedEstimations = allEstimations();
        var expected = expectedEstimations
            .map(e -> e.getOne() + "," + e.getTwo().treeView)
            .collect(joining(System.lineSeparator()));

        var actual = run("-n", 42, "-r", 1337, "--tree");
        assertEquals(expected.strip(), actual);
    }

    @Test
    void estimateAllAvailableProceduresInTreeInJsonMode() {
        var expectedEstimations = allEstimations();
        var expected = expectedEstimations
            .map(e -> expectedJson(e.getTwo(), e.getOne()))
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

    private static Stream<Pair<String, MemoryEstimateResult>> communityDetectionEstimations() {
        return sorted(Stream.of(
            runEstimation(K1ColoringMutateProc.class, "mutateEstimate", "mutateProperty", "foo"),
            runEstimation(K1ColoringStatsProc.class, "estimate"),
            runEstimation(K1ColoringStreamProc.class, "estimate"),
            runEstimation(K1ColoringWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(ModularityOptimizationMutateProc.class, "mutateEstimate", "mutateProperty", "foo"),
            runEstimation(ModularityOptimizationStreamProc.class, "estimate"),
            runEstimation(ModularityOptimizationWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(LabelPropagationMutateProc.class, "mutateEstimate", "mutateProperty", "foo"),
            runEstimation(LabelPropagationStatsProc.class, "estimateStats"),
            runEstimation(LabelPropagationStreamProc.class, "estimate"),
            runEstimation(LabelPropagationWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(LocalClusteringCoefficientMutateProc.class, "estimate", "mutateProperty", "foo"),
            runEstimation(LocalClusteringCoefficientStatsProc.class, "estimateStats"),
            runEstimation(LocalClusteringCoefficientStreamProc.class, "estimateStats"),
            runEstimation(LocalClusteringCoefficientWriteProc.class, "estimateStats", "writeProperty", "foo"),

            runEstimation(LouvainMutateProc.class, "estimate", "mutateProperty", "foo"),
            runEstimation(LouvainStatsProc.class, "estimateStats"),
            runEstimation(LouvainStreamProc.class, "estimate"),
            runEstimation(LouvainWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(TriangleCountMutateProc.class, "estimate", "mutateProperty", "foo"),
            runEstimation(TriangleCountStatsProc.class, "estimateStats"),
            runEstimation(TriangleCountStreamProc.class, "estimateStats"),
            runEstimation(TriangleCountWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(WccMutateProc.class, "mutateEstimate", "mutateProperty", "foo"),
            runEstimation(WccStatsProc.class, "statsEstimate"),
            runEstimation(WccStreamProc.class, "streamEstimate"),
            runEstimation(WccWriteProc.class, "writeEstimate", "writeProperty", "foo")
        ));
    }

    private static Stream<Pair<String, MemoryEstimateResult>> centralityEstimations() {
        return sorted(Stream.of(
            runEstimation(ArticleRankMutateProc.class, "estimate", "mutateProperty", "foo"),
            runEstimation(ArticleRankStatsProc.class, "estimateStats"),
            runEstimation(ArticleRankStreamProc.class, "estimate"),
            runEstimation(ArticleRankWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(BetweennessCentralityMutateProc.class, "estimate", "mutateProperty", "foo"),
            runEstimation(BetweennessCentralityStatsProc.class, "estimate"),
            runEstimation(BetweennessCentralityStreamProc.class, "estimate"),
            runEstimation(BetweennessCentralityWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(DegreeCentralityMutateProc.class, "estimate", "mutateProperty", "foo"),
            runEstimation(DegreeCentralityStatsProc.class, "estimate"),
            runEstimation(DegreeCentralityStreamProc.class, "estimate"),
            runEstimation(DegreeCentralityWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(EigenvectorMutateProc.class, "estimate", "mutateProperty", "foo"),
            runEstimation(EigenvectorStatsProc.class, "estimateStats"),
            runEstimation(EigenvectorStreamProc.class, "estimate"),
            runEstimation(EigenvectorWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(PageRankMutateProc.class, "estimate", "mutateProperty", "foo"),
            runEstimation(PageRankStatsProc.class, "estimateStats"),
            runEstimation(PageRankStreamProc.class, "estimate"),
            runEstimation(PageRankWriteProc.class, "estimate", "writeProperty", "foo")
        ));
    }

    private static Stream<Pair<String, MemoryEstimateResult>> similarityEstimations() {
        return sorted(Stream.of(
            runEstimation(
                KnnMutateProc.class, "estimateMutate",
                "nodeWeightProperty",
                "foo",
                "mutateProperty",
                "foo",
                "mutateRelationshipType",
                "bar"
            ),
            runEstimation(
                KnnStatsProc.class, "estimateStats",
                "nodeWeightProperty",
                "foo"
            ),
            runEstimation(
                KnnStreamProc.class, "estimate",
                "nodeWeightProperty",
                "foo"
            ),
            runEstimation(
                KnnWriteProc.class, "estimateWrite",
                "nodeWeightProperty",
                "foo",
                "writeProperty",
                "foo",
                "writeRelationshipType",
                "bar"
            ),

            runEstimation(
                NodeSimilarityMutateProc.class, "estimateMutate",
                "mutateProperty",
                "foo",
                "mutateRelationshipType",
                "bar"
            ),
            runEstimation(NodeSimilarityStatsProc.class, "estimateStats"),
            runEstimation(NodeSimilarityStreamProc.class, "estimate"),
            runEstimation(
                NodeSimilarityWriteProc.class, "estimateWrite",
                "writeProperty",
                "foo",
                "writeRelationshipType",
                "bar"
            )
        ));
    }

    private static Stream<Pair<String, MemoryEstimateResult>> pathFindingEstimations() {
        return sorted(Stream.of(
            runEstimation(AllShortestPathsDijkstraStreamProc.class, "streamEstimate",
                "sourceNode", 0L
            ),
            runEstimation(AllShortestPathsDijkstraWriteProc.class, "writeEstimate",
                "sourceNode", 0L,
                WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(AllShortestPathsDijkstraMutateProc.class, "mutateEstimate",
                "sourceNode", 0L,
                MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),

            runEstimation(ShortestPathAStarStreamProc.class, "streamEstimate",
                "sourceNode", 0L,
                "targetNode", 1L,
                ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY, "LAT",
                ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY, "LON"
            ),
            runEstimation(ShortestPathAStarWriteProc.class, "writeEstimate",
                "sourceNode", 0L,
                "targetNode", 1L,
                ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY, "LAT",
                ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY, "LON",
                WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(ShortestPathAStarMutateProc.class, "mutateEstimate",
                "sourceNode", 0L,
                "targetNode", 1L,
                ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY, "LAT",
                ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY, "LON",
                MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),

            runEstimation(ShortestPathDijkstraStreamProc.class, "streamEstimate",
                "sourceNode", 0L,
                "targetNode", 1L
            ),
            runEstimation(ShortestPathDijkstraWriteProc.class, "writeEstimate",
                "sourceNode", 0L,
                "targetNode", 1L,
                WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(ShortestPathDijkstraMutateProc.class, "mutateEstimate",
                "sourceNode", 0L,
                "targetNode", 1L,
                MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(ShortestPathYensStreamProc.class, "streamEstimate",
                "sourceNode", 0L,
                "targetNode", 1L,
                "k", 3
            ),
            runEstimation(ShortestPathYensWriteProc.class, "writeEstimate",
                "sourceNode", 0L,
                "targetNode", 1L,
                "k", 3,
                WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "FOO"
            ),
            runEstimation(ShortestPathYensMutateProc.class, "mutateEstimate",
                "sourceNode", 0L,
                "targetNode", 1L,
                "k", 3,
                MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "FOO"
            )
        ));
    }

    private static Stream<Pair<String, MemoryEstimateResult>> nodeEmbeddingEstimations() {
        return sorted(Stream.of(
            runEstimation(
                FastRPExtendedMutateProc.class, "estimate",
                "mutateProperty",
                "foo",
                "embeddingDimension",
                128,
                "propertyDimension",
                64
            ),
            runEstimation(FastRPExtendedStatsProc.class, "estimate", "embeddingDimension", 128, "propertyDimension", 64),
            runEstimation(FastRPExtendedStreamProc.class, "estimate", "embeddingDimension", 128, "propertyDimension", 64),
            runEstimation(
                FastRPExtendedWriteProc.class, "estimate",
                "writeProperty",
                "foo",
                "embeddingDimension",
                128,
                "propertyDimension",
                64
            ),

            runEstimation(Node2VecMutateProc.class, "estimate", "mutateProperty", "foo"),
            runEstimation(Node2VecStreamProc.class, "estimate"),
            runEstimation(Node2VecWriteProc.class, "estimate", "writeProperty", "foo"),

            runEstimation(FastRPMutateProc.class, "estimate", "mutateProperty", "foo", "embeddingDimension", 128),
            runEstimation(FastRPStatsProc.class, "estimate", "embeddingDimension", 128),
            runEstimation(FastRPStreamProc.class, "estimate", "embeddingDimension", 128),
            runEstimation(FastRPWriteProc.class, "estimate", "writeProperty", "foo", "embeddingDimension", 128)
        ));
    }

    private static Stream<Pair<String, MemoryEstimateResult>> machineLearningEstimations() {
        EstimationCli.addModelWithFeatures("", "model", List.of("a", "b"));
        var result = Stream.of(
            runEstimation(
                NodeClassificationPredictMutateProc.class, "estimate",
                "modelName",
                "model",
                "mutateProperty",
                "foo"
            ),
            runEstimation(NodeClassificationPredictStreamProc.class, "estimate", "modelName", "model"),
            runEstimation(
                NodeClassificationPredictWriteProc.class, "estimate",
                "modelName",
                "model",
                "writeProperty",
                "foo"
            ),
            runEstimation(
                NodeClassificationTrainProc.class, "estimate",
                "holdoutFraction", 0.2,
                "validationFolds", 5,
                "params", List.of(
                    Map.of("penalty", 0.0625),
                    Map.of("penalty", 0.125)
                ),
                "metrics", List.of("F1_MACRO"),
                "targetProperty", "target",
                "modelName", "model"
            )
        );
        ModelCatalog.removeAllLoadedModels();
        return sorted(result);
    }

    private static Stream<Pair<String, MemoryEstimateResult>> uncategorizedEstimations() {
        return sorted(Stream.of(
            Tuples.pair("gds.graph.create.estimate", graphCreateEstimate(false)),
            Tuples.pair("gds.graph.create.cypher.estimate", graphCreateEstimate(true))
        ));
    }

    private static Stream<Pair<String, MemoryEstimateResult>> allEstimations() {
        return sorted(
            communityDetectionEstimations(),
            centralityEstimations(),
            similarityEstimations(),
            pathFindingEstimations(),
            nodeEmbeddingEstimations(),
            machineLearningEstimations(),
            uncategorizedEstimations()
        );
    }

    @SafeVarargs
    private static Stream<Pair<String, MemoryEstimateResult>> sorted(Stream<Pair<String, MemoryEstimateResult>>... estimations) {
        return Stream.of(estimations)
            .flatMap(identity())
            .sorted(Comparator.comparing(Pair::getOne, String.CASE_INSENSITIVE_ORDER));
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

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final MethodType ESTIMATE_TYPE = MethodType.methodType(Stream.class, Object.class, Map.class);

    private static Pair<String, MemoryEstimateResult> runEstimation(
        Class<?> procClass,
        String methodName,
        Object... config
    ) {
        try {
            var handle = LOOKUP.findVirtual(procClass, methodName, ESTIMATE_TYPE);
            return runEstimation(handle, config);
        } catch (Throwable e) {
            throw new IllegalArgumentException(
                "Cannot access the underlying @Procedure annotated method of the provided method handle. It needs to be a direct handle. " +
                "See https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/invoke/MethodHandleInfo.html#directmh",
                e
            );
        }
    }

    private static Pair<String, MemoryEstimateResult> runEstimation(
        MethodHandle estimationMethod,
        Object... config
    ) throws Throwable {
        Map<String, Object> configMap = new HashMap<>(Map.of(
            "nodeCount", 42L,
            "relationshipCount", 1337L,
            "nodeProjection", "*",
            "relationshipProjection", "*"
        ));
        for (int i = 0; i < config.length; i += 2) {
            configMap.put(String.valueOf(config[i]), config[i + 1]);
        }
        var info = LOOKUP.revealDirect(estimationMethod);
        var method = info.reflectAs(Method.class, LOOKUP);
        var procClass = info.getDeclaringClass();
        var procedure = method.getAnnotation(Procedure.class);
        if (procedure == null) {
            throw new IllegalArgumentException("Method " + info.getName() + "is not annotated with @Procedure");
        }
        var procName = procedure.name();
        if (procName.isEmpty()) {
            procName = procedure.value();
        }

        var instance = procClass.getDeclaredConstructor().newInstance();
        var result = (Stream<MemoryEstimateResult>) estimationMethod.invoke(instance, configMap, Map.of());
        var estimation = result.iterator().next();

        return Tuples.pair(procName, estimation);
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
