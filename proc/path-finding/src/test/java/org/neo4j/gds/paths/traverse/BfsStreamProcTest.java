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
package org.neo4j.gds.paths.traverse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Graph:
 *
 *     (b)   (e)
 *    /  \  /  \
 * >(a)  (d)  ((g))
 *    \  /  \  /
 *    (c)   (f)
 */
class BfsStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a'})" +
        ", (b:Node {name:'b'})" +
        ", (c:Node {name:'c'})" +
        ", (d:Node {name:'d'})" +
        ", (e:Node {name:'e'})" +
        ", (f:Node {name:'f'})" +
        ", (g:Node {name:'g'})" +
        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE2]->(a)" +
        ", (a)-[:TYPE]->(c)" +
        ", (b)-[:TYPE]->(d)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(e)" +
        ", (d)-[:TYPE]->(f)" +
        ", (e)-[:TYPE]->(g)" +
        ", (f)-[:TYPE]->(g)";


    private static final String REVERSE_GRAPH_NAME = DEFAULT_GRAPH_NAME + "_reverse";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(BfsStreamProc.class, GraphProjectProc.class);
    }

    @Test
    void testFindAnyOf() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE")
            .yields();
        runQuery(createQuery);

        long id = idFunction.of("a");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("bfs")
            .streamMode()
            .addParameter("sourceNode", id)
            .addParameter("targetNodes", List.of(idFunction.of("e"), idFunction.of("f")))
            .yields("sourceNode", "nodeIds");

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("sourceNode").longValue()).isEqualTo(id);
            var nodeIds = row.get("nodeIds");

            assertThat(nodeIds)
                .isEqualTo(Stream.of("a", "b", "c", "d", "e").map(idFunction::of).collect(Collectors.toList()));
        });
    }

    @Test
    void testMaxDepthOut() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE")
            .yields();
        runQuery(createQuery);

        long source = idFunction.of("a");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("bfs")
            .streamMode()
            .addParameter("sourceNode", source)
            .addParameter("maxDepth", 2)
            .yields("sourceNode", "nodeIds");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(row.getNumber("sourceNode").longValue(), source);
            var nodeIds = row.get("nodeIds");
            assertThat(nodeIds).isEqualTo(
                Stream.of("a", "b", "c", "d").map(idFunction::of).collect(Collectors.toList())
            );
        });
    }

    @Test
    void testMaxDepthIn() {
        var createReverseGraphQuery = GdsCypher.call(REVERSE_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", Orientation.REVERSE)
            .yields();
        runQuery(createReverseGraphQuery);

        long source = idFunction.of("g");
        String query = GdsCypher.call(REVERSE_GRAPH_NAME)
            .algo("bfs")
            .streamMode()
            .addParameter("sourceNode", source)
            .addParameter("maxDepth", 2)
            .yields("sourceNode, nodeIds");
        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("sourceNode").longValue()).isEqualTo(source);
            var nodeIds = row.get("nodeIds");
            assertThat(nodeIds).isEqualTo(
                Stream.of("g", "e", "f", "d").map(idFunction::of).collect(Collectors.toList())
            );
        });
    }

    @Test
    void testWithoutEarlyTermination() {
        var createReverseGraphQuery = GdsCypher.call(REVERSE_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", Orientation.REVERSE)
            .yields();
        runQuery(createReverseGraphQuery);

        long source = idFunction.of("g");
        String query = GdsCypher.call(REVERSE_GRAPH_NAME)
            .algo("bfs")
            .streamMode()
            .addParameter("sourceNode", source)
            .yields("sourceNode", "nodeIds");
        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("sourceNode").longValue()).isEqualTo(source);

            var nodeIds = row.get("nodeIds");

            assertThat(nodeIds)
                .asList()
                .containsExactlyElementsOf(
                    Stream.of("g", "e", "f", "d", "b", "c", "a")
                        .map(idFunction::of)
                        .collect(Collectors.toList())
                );
        });
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("pathQueryBuilders")
    void testPathField(Function<GdsCypher.ParametersBuildStage, String> queryProvider, String displayName) {
        var createReverseGraphQuery = GdsCypher.call(REVERSE_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", Orientation.REVERSE)
            .yields();
        runQuery(createReverseGraphQuery);

        var parametersBuildStage = GdsCypher.call(REVERSE_GRAPH_NAME)
            .algo("bfs")
            .streamMode()
            .addParameter("sourceNode", idFunction.of("g"));
        String query = queryProvider.apply(parametersBuildStage);

        runQueryWithRowConsumer(query, row -> {
            var path = row.getPath("path");

            assertThat(path.length()).isEqualTo(6);

            var nodeIds = new ArrayList<Long>();
            path.nodes().forEach(node -> {
                nodeIds.add(node.getId());
            });

            assertThat(nodeIds)
                .containsExactlyElementsOf(Stream.of("g", "e", "f", "d", "b", "c", "a").map(idFunction::of).collect(Collectors.toList()));
        });
    }

    static Stream<Arguments> pathQueryBuilders() {
        return Stream.of(
            Arguments.of((Function<GdsCypher.ParametersBuildStage, String>) GdsCypher.ParametersBuildStage::yields, "No yield fields specified"),
            Arguments.of((Function<GdsCypher.ParametersBuildStage, String>) stage -> stage.yields("path"), "Only `path` yield field")
        );
    }

    @Test
    void failOnInvalidSourceNode() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("bfs")
            .streamMode()
            .addParameter("sourceNode", 42)
            .yields();

        assertError(query, "Source node does not exist in the in-memory graph: `42`");
    }

    @Test
    void failOnInvalidEndNode() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("bfs")
            .streamMode()
            .addParameter("sourceNode", 0)
            .addParameter("targetNodes", List.of(0, 42, 1))
            .yields();

        assertError(query, "Target nodes do not exist in the in-memory graph: ['42']");
    }

}
