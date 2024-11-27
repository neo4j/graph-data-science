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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.wcc.WccStub;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.REVERSE;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.TestSupport.toArguments;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
public class ComponentPropertyNodeSimilarityTest {

    @GdlGraph(graphNamePrefix = "natural", orientation = NATURAL, idOffset = 0)
    @GdlGraph(graphNamePrefix = "reverse", orientation = REVERSE, idOffset = 0)
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:Person {compid: 0})" +
            ", (b:Person {compid: 0})" +
            ", (c:Person {compid: 0})" +
            ", (d:Person {compid: 0})" +
            ", (e:Person {compid: 1})" +
            ", (i1:Item {compid: 0})" +
            ", (i2:Item {compid: 0})" +
            ", (i3:Item {compid: 0})" +
            ", (i4:Item {compid: 1})" +
            ", (i5:Item {compid: 1})" +
            ", (a)-[:LIKES {prop: 1.0}]->(i1)" +
            ", (a)-[:LIKES {prop: 1.0}]->(i2)" +
            ", (a)-[:LIKES {prop: 2.0}]->(i3)" +
            ", (b)-[:LIKES {prop: 1.0}]->(i1)" +
            ", (b)-[:LIKES {prop: 1.0}]->(i2)" +
            ", (c)-[:LIKES {prop: 1.0}]->(i3)" +
            ", (d)-[:LIKES {prop: 0.5}]->(i1)" +
            ", (d)-[:LIKES {prop: 1.0}]->(i2)" +
            ", (d)-[:LIKES {prop: 1.0}]->(i3)" +
            ", (e)-[:LIKES {prop: 1.0}]->(i4)" +
            ", (e)-[:LIKES {prop: 1.0}]->(i5)";
    private static final Collection<String> EXPECTED_OUTGOING_COMP_OPT = new HashSet<>();
    private static final Collection<String> EXPECTED_INCOMING_COMP_OPT = new HashSet<>();

    static {
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(0, 3, 1.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(1, 2, 0.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(1, 3, 2 / 3.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(2, 3, 1 / 3.0));
        // Add results in reverse direction because topK
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(2, 0, 1 / 3.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(3, 0, 1.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(2, 1, 0.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(3, 1, 2 / 3.0));
        EXPECTED_OUTGOING_COMP_OPT.add(resultString(3, 2, 1 / 3.0));

        EXPECTED_INCOMING_COMP_OPT.add(resultString(9, 8, 1.0));
        EXPECTED_INCOMING_COMP_OPT.add(resultString(5, 6, 1.0));
        EXPECTED_INCOMING_COMP_OPT.add(resultString(5, 7, 1 / 2.0));
        EXPECTED_INCOMING_COMP_OPT.add(resultString(6, 7, 1 / 2.0));
        // Add results in reverse direction because topK
        EXPECTED_INCOMING_COMP_OPT.add(resultString(8, 9, 1.0));
        EXPECTED_INCOMING_COMP_OPT.add(resultString(6, 5, 1.0));
        EXPECTED_INCOMING_COMP_OPT.add(resultString(7, 5, 1 / 2.0));
        EXPECTED_INCOMING_COMP_OPT.add(resultString(7, 6, 1 / 2.0));
    }

    @Inject
    private TestGraph naturalGraph;
    @Inject
    private TestGraph reverseGraph;

    private static String resultString(long node1, long node2, double similarity) {
        return formatWithLocale("%d,%d %f", node1, node2, similarity);
    }

    private static String resultString(SimilarityResult result) {
        return resultString(result.node1, result.node2, result.similarity);
    }

    private static Stream<Integer> concurrencies() {
        return Stream.of(1, 4);
    }

    static Stream<Arguments> supportedLoadAndComputeDirections() {
        Stream<Arguments> directions = Stream.of(
            arguments(NATURAL),
            arguments(REVERSE)
        );
        return crossArguments(() -> directions, toArguments(ComponentPropertyNodeSimilarityTest::concurrencies));
    }


    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldOptimizeForDistinctComponentsProperty(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        var parameters = new NodeSimilarityParameters(
            new JaccardSimilarityComputer(0.0),
            1,
            Integer.MAX_VALUE,
            10,
            0,
            true,
            false,
            true,
            "compid"
        );

        var nodeSimilarity = new NodeSimilarity(
            graph,
            parameters,
            new Concurrency(concurrency),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            NodeFilter.ALLOW_EVERYTHING,
            NodeFilter.ALLOW_EVERYTHING,
            TerminationFlag.RUNNING_TRUE,
            new WccStub(TerminationFlag.RUNNING_TRUE, new AlgorithmMachinery())
        );

        Set<String> result = nodeSimilarity
            .compute()
            .streamResult()
            .map(ComponentPropertyNodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertEquals(orientation == REVERSE ? EXPECTED_INCOMING_COMP_OPT : EXPECTED_OUTGOING_COMP_OPT, result);
    }
}
