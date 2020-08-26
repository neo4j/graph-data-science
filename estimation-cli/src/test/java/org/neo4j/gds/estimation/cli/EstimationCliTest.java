/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.pagerank.PageRankStreamProc;
import org.neo4j.graphalgo.results.MemoryEstimateResult;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

final class EstimationCliTest {

    private static final String PR_ESTIMATE = "gds.pageRank.stream.estimate";

    @ParameterizedTest
    @CsvSource({
        "--nodes, --relationships",
        "-n, -r",
    })
    void runsEstimation(String nodeArg, String relArg) {
        var actual = run(PR_ESTIMATE, nodeArg, 42, relArg, 1337);
        var expected = pageRankEstimate(42, 1337);

        assertEquals(expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"--labels", "-l"})
    void runsEstimationWithLabels(String labelsArg) {
        var actual = run(PR_ESTIMATE, "--nodes", 42, "--relationships", 1337, labelsArg, 21);
        var expected = pageRankEstimate(
            42, 1337,
            "nodeProjection", listOfIdentifiers(21)
        );

        assertEquals(expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"--node-properties", "-np"})
    void runsEstimationWithNodeProperties(String nodePropsArg) {
        var actual = run(PR_ESTIMATE, "--nodes", 42, "--relationships", 1337, nodePropsArg, 21);
        var expected = pageRankEstimate(
            42, 1337,
            "nodeProperties", listOfIdentifiers(21)
        );

        assertEquals(expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"--relationship-properties", "-rp"})
    void runsEstimationWithRelationshipProperties(String relPropsArg) {
        var actual = run(PR_ESTIMATE, "--nodes", 42, "--relationships", 1337, relPropsArg, 21);
        var expected = pageRankEstimate(
            42, 1337,
            "relationshipProperties", listOfIdentifiers(21)
        );

        assertEquals(expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @Test
    void runsEstimationWithConcurrency() {
        var actual = run(PR_ESTIMATE, "--nodes", 42, "--relationships", 1337, "-c", "readConcurrency=21");
        var expected = pageRankEstimate(
            42, 1337,
            "readConcurrency", 21
        );

        assertEquals(expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @Test
    void estimatesGraphCreate() {
        var actual = run("gds.graph.create", "--nodes", 42, "--relationships", 1337);
        var expected = graphCreateEstimate(42, 1337, false);

        assertEquals(expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @Test
    void estimatesGraphCreateCypher() {
        var actual = run("gds.graph.create.cypher", "--nodes", 42, "--relationships", 1337);
        var expected = graphCreateEstimate(42, 1337, true);

        assertEquals(expected.bytesMin + "," + expected.bytesMax, actual);
    }

    @Test
    void printsTree() {
        var actual = run(PR_ESTIMATE, "-n", 42, "-r", 1337, "--tree");
        var expected = pageRankEstimate(42, 1337);

        assertEquals(expected.treeView.strip(), actual);
    }

    @Test
    void printsJson() {
        var actual = run(PR_ESTIMATE, "-n", 42, "-r", 1337, "--json");
        var expected = pageRankEstimate(42, 1337);

        var expectedJsonTemplate =
            "{\n" +
            "  \"bytes_min\" : %d,\n" +
            "  \"bytes_max\" : %d,\n" +
            "  \"min_memory\" : \"%s\",\n" +
            "  \"max_memory\" : \"%s\",\n" +
            "  \"procedure\" : \"gds.pagerank.stream.estimate\",\n" +
            "  \"node_count\" : 42,\n" +
            "  \"relationship_count\" : 1337,\n" +
            "  \"label_count\" : 0,\n" +
            "  \"relationship_type_count\" : 0,\n" +
            "  \"node_property_count\" : 0,\n" +
            "  \"relationship_property_count\" : 0\n" +
            "}";
        var expectedJson = formatWithLocale(
            expectedJsonTemplate,
            expected.bytesMin,
            expected.bytesMax,
            humanReadable(expected.bytesMin),
            humanReadable(expected.bytesMax)
        );

        assertEquals(expectedJson, actual);
    }

    @Test
    void procIsMandatory() {
        var actual = assertThrows(ExecutionFailed.class, () -> run("-n", 42, "-r", 1337));

        assertEquals(2, actual.exitCode);
        assertEquals("Error: Missing required argument(s): <procedure>", actual.stderr.lines().iterator().next());
    }

    @Test
    void nodeCountIsMandatory() {
        var actual = assertThrows(ExecutionFailed.class, () -> run(PR_ESTIMATE, "-r", 1337));

        assertEquals(2, actual.exitCode);
        assertEquals("Error: Missing required argument(s): --nodes=<nodeCount>", actual.stderr.lines().iterator().next());
    }

    @Test
    void relationshipCountIsMandatory() {
        var actual = assertThrows(ExecutionFailed.class, () -> run(PR_ESTIMATE, "-n", 42));

        assertEquals(2, actual.exitCode);
        assertEquals(
            "Error: Missing required argument(s): --relationships=<relationshipCount>",
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
        assertTrue(
            actual.stderr.lines().iterator().next().startsWith("Error: expected only one match but got")
        );
    }

    private static final List<String> PROCEDURES = List.of(
        "gds.beta.k1coloring.mutate.estimate",
        "gds.beta.k1coloring.stats.estimate",
        "gds.beta.k1coloring.stream.estimate",
        "gds.beta.k1coloring.write.estimate",

        "gds.beta.modularityOptimization.mutate.estimate",
        "gds.beta.modularityOptimization.stream.estimate",
        "gds.beta.modularityOptimization.write.estimate",

        "gds.betweenness.mutate.estimate",
        "gds.betweenness.stats.estimate",
        "gds.betweenness.stream.estimate",
        "gds.betweenness.write.estimate",

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

        "gds.testProc.test.estimate",

        "gds.triangleCount.mutate.estimate",
        "gds.triangleCount.stats.estimate",
        "gds.triangleCount.stream.estimate",
        "gds.triangleCount.write.estimate",

        "gds.wcc.mutate.estimate",
        "gds.wcc.stats.estimate",
        "gds.wcc.stream.estimate",
        "gds.wcc.write.estimate"
    );

    @Test
    void listAllAvailableProcedures() {
        var actual = run("--list-available");
        var expected = PROCEDURES.stream().collect(joining(System.lineSeparator()));

        assertEquals(expected, actual);
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

            exitCode = EstimationCli.run(arguments);

        } finally {
            System.setErr(originalErr);
            System.setOut(originalOut);
        }


        if (exitCode != 0) {
            throw new ExecutionFailed(exitCode, stderr.toString(StandardCharsets.UTF_8));
        }

        return stdout.toString(StandardCharsets.UTF_8).strip();
    }

    private static MemoryEstimateResult pageRankEstimate(long nodeCount, long relationshipCount, Object... config) {
        var pr = new PageRankStreamProc();
        Map<String, Object> configMap = new HashMap<>(Map.of(
            "nodeCount", nodeCount,
            "relationshipCount", relationshipCount,
            "nodeProjection", "*",
            "relationshipProjection", "*"
        ));
        for (int i = 0; i < config.length; i += 2) {
            configMap.put(String.valueOf(config[i]), config[i + 1]);
        }
        return pr.estimate(configMap, Map.of()).iterator().next();
    }

    private static MemoryEstimateResult graphCreateEstimate(long nodeCount, long relationshipCount, boolean cypher) {
        Map<String, Object> config = Map.of(
            "nodeCount", nodeCount,
            "relationshipCount", relationshipCount
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
