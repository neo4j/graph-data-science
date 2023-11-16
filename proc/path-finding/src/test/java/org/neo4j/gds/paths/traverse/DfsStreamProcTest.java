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
import java.util.Arrays;
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
class DfsStreamProcTest extends BaseProcTest {

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
        ", (a)-[:TYPE {cost:2.0}]->(b)" +
        ", (b)-[:TYPE2 {cost:2.0}]->(a)" +
        ", (a)-[:TYPE {cost:1.0}]->(c)" +
        ", (b)-[:TYPE {cost:1.0}]->(d)" +
        ", (c)-[:TYPE {cost:2.0}]->(d)" +
        ", (d)-[:TYPE {cost:1.0}]->(e)" +
        ", (d)-[:TYPE {cost:2.0}]->(f)" +
        ", (e)-[:TYPE {cost:2.0}]->(g)" +
        ", (f)-[:TYPE {cost:1.0}]->(g)";

    private static final String REVERSE_GRAPH_NAME = DEFAULT_GRAPH_NAME + "_reverse";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(DfsStreamProc.class, GraphProjectProc.class);
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
            .algo("dfs")
            .streamMode()
            .addParameter("sourceNode", id)
            .addParameter("targetNodes", Arrays.asList(idFunction.of("e"), idFunction.of("f")))
            .yields("sourceNode, nodeIds");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(row.getNumber("sourceNode").longValue(), id);
            var nodeIds = row.get("nodeIds");

            // We can't predict the traversal order deterministically => check that we've taken one of the paths
            assertThat(nodeIds).isIn(
                Stream.of("a", "b", "d", "e").map(idFunction::of).collect(Collectors.toList()),
                Stream.of("a", "c", "d", "f").map(idFunction::of).collect(Collectors.toList())
            );
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

        long id = idFunction.of("a");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("dfs")
            .streamMode()
            .addParameter("sourceNode", id)
            .addParameter("maxDepth", 2)
            .yields("sourceNode, nodeIds");
        runQueryWithRowConsumer(query, row -> {
            assertEquals(row.getNumber("sourceNode").longValue(), id);
            var nodeIds = row.get("nodeIds");
            assertThat(nodeIds).isEqualTo(
                Stream.of("a", "c", "d", "b").map(idFunction::of).collect(Collectors.toList())
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

        long id = idFunction.of("g");
        String query = GdsCypher.call(REVERSE_GRAPH_NAME)
            .algo("dfs")
            .streamMode()
            .addParameter("sourceNode", id)
            .addParameter("maxDepth", 2)
            .yields("sourceNode, nodeIds");
        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("sourceNode").longValue()).isEqualTo(id);
            var nodeIds = row.get("nodeIds");
            assertThat(nodeIds).isEqualTo(
                Stream.of("g", "f", "d", "e").map(idFunction::of).collect(Collectors.toList())
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

        long id = idFunction.of("g");
        String query = GdsCypher.call(REVERSE_GRAPH_NAME)
            .algo("dfs")
            .streamMode()
            .addParameter("sourceNode", id)
            .yields("sourceNode, nodeIds");
        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("sourceNode").longValue()).isEqualTo(id);
            var nodeIds = row.get("nodeIds");

            // We can't predict the traversal order deterministically => check the possible combinations
            assertThat(nodeIds).isIn(
                Stream.of("g", "e", "d", "b", "a", "c", "f").map(idFunction::of).collect(Collectors.toList()),
                Stream.of("g", "e", "d", "c", "a", "b", "f").map(idFunction::of).collect(Collectors.toList()),
                Stream.of("g", "f", "d", "b", "a", "c", "e").map(idFunction::of).collect(Collectors.toList()),
                Stream.of("g", "f", "d", "c", "a", "b", "e").map(idFunction::of).collect(Collectors.toList())
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
            .algo("dfs")
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

            // We can't predict the traversal order deterministically => check the possible combinations
            assertThat(nodeIds).isIn(
                Stream.of("g", "e", "d", "b", "a", "c", "f").map(idFunction::of).collect(Collectors.toList()),
                Stream.of("g", "e", "d", "c", "a", "b", "f").map(idFunction::of).collect(Collectors.toList()),
                Stream.of("g", "f", "d", "b", "a", "c", "e").map(idFunction::of).collect(Collectors.toList()),
                Stream.of("g", "f", "d", "c", "a", "b", "e").map(idFunction::of).collect(Collectors.toList())
            );
        });
    }

    static Stream<Arguments> pathQueryBuilders() {
        return Stream.of(
            Arguments.of((Function<GdsCypher.ParametersBuildStage, String>) GdsCypher.ParametersBuildStage::yields, "No yield fields specified"),
            Arguments.of((Function<GdsCypher.ParametersBuildStage, String>) stage -> stage.yields("path"), "Only `path` yield field")
        );
    }
    
}
