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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.core.huge.UnionGraph;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.Result.ResultRow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class TriangleCentralityNodeMappingProcTest extends BaseProcTest {

    // Leave the first database node unprojected and interleave Keep and Drop nodes
    // so database, root graph, and filtered graph IDs all need translation.
    // The selected graph is K4 plus e connected to a and b, giving nonuniform scores.
    // The extra triangle through excluded nodes must not contribute to those scores.
    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
        "(unprojected:Unprojected {name: 'unprojected', centrality: -7.0}), " +
        "(outside0:Drop {name: 'outside0', centrality: -7.0}), " +
        "(a:Keep {name: 'a', centrality: -7.0}), " +
        "(outside1:Drop {name: 'outside1', centrality: -7.0}), " +
        "(b:Keep {name: 'b', centrality: -7.0}), " +
        "(outside2:Drop {name: 'outside2', centrality: -7.0}), " +
        "(c:Keep {name: 'c', centrality: -7.0}), " +
        "(outside3:Drop {name: 'outside3', centrality: -7.0}), " +
        "(d:Keep {name: 'd', centrality: -7.0}), " +
        "(outside4:Drop {name: 'outside4', centrality: -7.0}), " +
        "(e:Keep {name: 'e', centrality: -7.0}), " +
        "(a)-[:T]->(b), (b)-[:T]->(c), (c)-[:T]->(a), " +
        "(a)-[:T]->(d), (b)-[:T]->(d), (c)-[:T]->(d), " +
        "(a)-[:T]->(e), (b)-[:T]->(e), " +
        "(a)-[:T]->(outside0), (outside0)-[:T]->(outside1), (outside1)-[:T]->(a)";

    private static final Map<String, Double> SCORES = Map.of(
        "a", 1.0, "b", 1.0, "c", 14.0 / 15, "d", 14.0 / 15, "e", 0.6
    );
    private static final Map<String, Long> TRIANGLE_COUNTS = Map.of("a", 4L, "b", 4L, "c", 3L, "d", 3L, "e", 1L);
    private static final Map<String, Long> TRIANGLE_NEIGHBORS = Map.of("a", 4L, "b", 4L, "c", 3L, "d", 3L, "e", 2L);
    private static final Map<String, Long> NEIGHBOR_SUMS = Map.of("a", 11L, "b", 11L, "c", 11L, "d", 11L, "e", 8L);

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            TriangleCentralityStreamProc.class,
            TriangleCentralityMutateProc.class,
            TriangleCentralityWriteProc.class
        );
    }

    private static Stream<Arguments> graphConfigurations() {
        return Stream.of(false, true).flatMap(union -> Stream.of(1, 4).flatMap(concurrency ->
            Stream.of(Long.MAX_VALUE, 3L).map(maxDegree -> Arguments.of(union, concurrency, maxDegree))
        ));
    }

    @ParameterizedTest(name = "union={0}, concurrency={1}, maxDegree={2}")
    @MethodSource("graphConfigurations")
    void shouldStreamOriginalNodeIds(boolean union, int concurrency, long maxDegree) {
        var config = projectAndConfigure(union, concurrency, maxDegree);
        var expectedNodes = new HashMap<Long, String>();
        SCORES.keySet().forEach(name -> expectedNodes.put(idFunction.of(name), name));

        var rows = runQueryWithRowConsumer(db, "CALL gds.triangleCentrality.stream('graph', $config)",
            Map.of("config", config), row -> {
                var name = expectedNodes.remove(row.getNumber("nodeId").longValue());
                assertThat(name).as("Each selected database node must be returned exactly once").isNotNull();
                assertScore(row.getNumber("triangleCentrality").doubleValue(), name, maxDegree);
                boolean limited = maxDegree != Long.MAX_VALUE;
                assertThat(row.getNumber("triangleCount").longValue())
                    .isEqualTo(limited ? (excludedByDegree(name) ? -1L : 0L) : TRIANGLE_COUNTS.get(name));
                assertThat(row.getNumber("triangleNeighbors").longValue())
                    .isEqualTo(limited ? 0L : TRIANGLE_NEIGHBORS.get(name));
                assertThat(row.getNumber("nonTriangleNeighbors").longValue())
                    .isEqualTo(limited && (name.equals("c") || name.equals("d")) ? 1L : 0L);
                assertThat(row.getNumber("triangleNeighborsTriangleSum").longValue())
                    .isEqualTo(limited ? 0L : NEIGHBOR_SUMS.get(name));
                assertThat(row.getNumber("nonTriangleNeighborsTriangleSum").longValue()).isZero();
            });

        assertThat(rows).isEqualTo(SCORES.size());
        assertThat(expectedNodes).isEmpty();
        assertDatabaseScores(Map.of());
    }

    @ParameterizedTest(name = "union={0}, concurrency={1}, maxDegree={2}")
    @MethodSource("graphConfigurations")
    void shouldMutateTheCorrectRootNodes(boolean union, int concurrency, long maxDegree) {
        var config = projectAndConfigure(union, concurrency, maxDegree);
        config.put("mutateProperty", "centrality");

        var rows = runQueryWithRowConsumer(db, "CALL gds.triangleCentrality.mutate('graph', $config)",
            Map.of("config", config), row -> assertSummary(row, maxDegree, 5));
        assertThat(rows).isEqualTo(1);

        var store = graphStore();
        assertThat(store.hasNodeProperty(NodeLabel.of("Keep"), "centrality")).isTrue();
        assertThat(store.hasNodeProperty(NodeLabel.of("Drop"), "centrality")).isFalse();
        var root = store.getUnion();
        var properties = root.nodeProperties("centrality");
        SCORES.keySet().forEach(name -> assertScore(
            properties.doubleValue(root.toMappedNodeId(idFunction.of(name))), name, maxDegree
        ));
        for (int i = 0; i < 5; i++) {
            assertThat(properties.doubleValue(root.toMappedNodeId(idFunction.of("outside" + i)))).isNaN();
        }
        assertDatabaseScores(Map.of());
    }

    @ParameterizedTest(name = "union={0}, concurrency={1}, maxDegree={2}")
    @MethodSource("graphConfigurations")
    void shouldWriteTheCorrectDatabaseNodes(boolean union, int concurrency, long maxDegree) {
        var config = projectAndConfigure(union, concurrency, maxDegree);
        config.put("writeProperty", "centrality");
        config.put("writeConcurrency", concurrency);

        var rows = runQueryWithRowConsumer(db, "CALL gds.triangleCentrality.write('graph', $config)",
            Map.of("config", config), row -> assertSummary(row, maxDegree, maxDegree == Long.MAX_VALUE ? 5 : 3));
        assertThat(rows).isEqualTo(1);

        assertDatabaseScores(maxDegree == Long.MAX_VALUE ? SCORES : Map.of("c", 0.0, "d", 0.0, "e", 0.0));
        assertThat(graphStore().hasNodeProperty("centrality")).isFalse();
    }

    private Map<String, Object> projectAndConfigure(boolean union, int concurrency, long maxDegree) {
        if (union) {
            // Triangles span types, and a duplicate edge checks union deduplication.
            runQuery("MATCH (b:Keep {name: 'b'})-[r:T]->(c:Keep {name: 'c'}) DELETE r CREATE (b)-[:U]->(c)");
            runQuery("MATCH (a:Keep {name: 'a'}), (b:Keep {name: 'b'}) CREATE (a)-[:U]->(b)");
        }
        var types = union ? List.of("T", "U") : List.of("T");
        var projection = new HashMap<String, Object>();
        types.forEach(type -> projection.put(type, Map.of("orientation", "UNDIRECTED")));
        runQuery("CALL gds.graph.project('graph', ['Keep', 'Drop'], $relationships)", Map.of("relationships", projection));

        var store = graphStore();
        var filtered = store.getGraph(
            List.of(NodeLabel.of("Keep")), types.stream().map(RelationshipType::of).toList(), Optional.empty()
        );
        if (union) {
            assertThat(filtered).isInstanceOf(UnionGraph.class);
            assertThat(((UnionGraph) filtered).isNodeFilteredGraph()).isTrue();
        } else {
            assertThat(filtered).isInstanceOf(NodeFilteredGraph.class);
        }
        // Guard against a fixture that accidentally leaves filtered IDs identical to root IDs.
        var root = store.getUnion();
        assertThat(SCORES.keySet()).anySatisfy(name -> assertThat(
            filtered.toMappedNodeId(idFunction.of(name))
        ).isNotEqualTo(root.toMappedNodeId(idFunction.of(name))));
        assertThat(SCORES.keySet()).anySatisfy(name -> assertThat(
            root.toMappedNodeId(idFunction.of(name))
        ).isNotEqualTo(idFunction.of(name)));

        return new HashMap<>(Map.of(
            "nodeLabels", List.of("Keep"), "relationshipTypes", types,
            "concurrency", concurrency, "maxDegree", maxDegree
        ));
    }

    private void assertScore(double actual, String name, long maxDegree) {
        if (maxDegree != Long.MAX_VALUE && excludedByDegree(name)) {
            assertThat(actual).as("centrality of %s", name).isNaN();
        } else {
            assertThat(actual).as("centrality of %s", name)
                .isCloseTo(maxDegree == Long.MAX_VALUE ? SCORES.get(name) : 0.0, within(1e-10));
        }
    }

    private static boolean excludedByDegree(String name) {
        return name.equals("a") || name.equals("b");
    }

    private void assertDatabaseScores(Map<String, Double> expected) {
        var remaining = new HashMap<>(expected);
        var rows = runQueryWithRowConsumer("MATCH (n) RETURN n.name AS name, n.centrality AS centrality", row -> {
            var name = row.getString("name");
            assertThat(row.getNumber("centrality").doubleValue()).as("database property of %s", name)
                .isCloseTo(expected.getOrDefault(name, -7.0), within(1e-10));
            remaining.remove(name);
        });
        assertThat(rows).isEqualTo(11);
        assertThat(remaining).isEmpty();
    }

    private GraphStore graphStore() {
        return GraphStoreCatalog.get(getUsername(), DatabaseId.of(db.databaseName()), "graph").graphStore();
    }

    private static void assertSummary(ResultRow row, long maxDegree, long propertiesWritten) {
        assertThat(row.getNumber("nodeCount").longValue()).isEqualTo(5);
        assertThat(row.getNumber("globalTriangleCount").longValue()).isEqualTo(maxDegree == Long.MAX_VALUE ? 5 : 0);
        assertThat(row.getNumber("nodePropertiesWritten").longValue()).isEqualTo(propertiesWritten);
        @SuppressWarnings("unchecked")
        var distribution = (Map<String, Object>) row.get("centralityDistribution");
        assertThat(distribution).hasSize(9);
        assertThat((double) distribution.get("mean"))
            .isCloseTo(maxDegree == Long.MAX_VALUE ? 67.0 / 75 : 0.0, within(1e-5));
    }
}
