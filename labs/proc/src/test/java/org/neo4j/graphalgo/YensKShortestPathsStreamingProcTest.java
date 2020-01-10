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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.shortestpath.KShortestPathsProc;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;

/**
 * Graph:
 *
 *            (0)
 *          /  |  \
 *       (4)--(5)--(1)
 *         \  /  \ /
 *         (3)---(2)
 */
class YensKShortestPathsStreamingProcTest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE (f:Node {name:'f'})\n" +
                "CREATE (a)-[:TYPE {cost:1.0}]->(b)\n" +
                "CREATE (a)-[:TYPE {cost:5.0}]->(d)\n" +
                "CREATE (a)-[:TYPE {cost:7.0}]->(e)\n" +
                "CREATE (e)-[:TYPE {cost:3.0}]->(f)\n" +
                "CREATE (f)-[:TYPE {cost:4.0}]->(d)\n" +
                "CREATE (b)-[:TYPE {cost:2.0}]->(d)\n";

        runQuery(cypher);
        runQuery("MATCH (c:Node {name: 'c'}) DELETE c");
        registerProcedures(KShortestPathsProc.class);
    }

    @AfterEach
    void teardownGraph() {
        db.shutdown();
    }

    @Test
    void shouldReturnNodesAndCosts() {
        String algoCall = GdsCypher.call()
            .withRelationshipProperty("cost")
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds.alpha.kShortestPaths")
            .streamMode()
            .addVariable("startNode", "a")
            .addVariable("endNode", "d")
            .addParameter("k", 42)
            .addParameter("weightProperty", "cost")
            .yields("index", "sourceNodeId", "targetNodeId", "nodeIds", "costs");

        String cypher = String.format(
            "MATCH (a:Node{name:'a'}), (d:Node{name:'d'}) %s RETURN *",
            algoCall
        );

        Map<Long, List<Long>> expectedNodes = new HashMap<>();
        expectedNodes.put(0L, getNodeIds("a", "b", "d"));
        expectedNodes.put(1L, getNodeIds("a", "d"));
        expectedNodes.put(2L, getNodeIds("a", "e", "f", "d"));

        Map<Long, List<Double>> expectedCosts = new HashMap<>();
        expectedCosts.put(0L, Arrays.asList(1.0D, 2.0D));
        expectedCosts.put(1L, Arrays.asList(5.0D));
        expectedCosts.put(2L, Arrays.asList(7.0D, 3.0, 4.0));

        // 9 possible paths without loop
        runQueryWithRowConsumer(cypher, row -> {
            long rowIndex = row.getNumber("index").longValue();
            assertEquals(expectedNodes.get(rowIndex), row.get("nodeIds"));
            assertEquals(expectedCosts.get(rowIndex), row.get("costs"));
        });
    }

    @Test
    void shouldReturnPaths() {
        String algoCall = GdsCypher.call()
            .withRelationshipProperty("cost")
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds.alpha.kShortestPaths")
            .streamMode()
            .addVariable("startNode", "a")
            .addVariable("endNode", "d")
            .addParameter("k", 42)
            .addParameter("weightProperty", "cost")
            .addParameter("path", true)
            .yields("index", "sourceNodeId", "targetNodeId", "nodeIds", "costs", "path");

        String cypher = String.format(
            "MATCH (a:Node{name:'a'}), (d:Node{name:'d'}) %s RETURN *",
            algoCall
        );

        Map<Long, List<Node>> expectedNodes = new HashMap<>();
        expectedNodes.put(0L, getNodes("a", "b", "d"));
        expectedNodes.put(1L, getNodes("a", "d"));
        expectedNodes.put(2L, getNodes("a", "e", "f", "d"));

        Map<Long, List<Double>> expectedCosts = new HashMap<>();
        expectedCosts.put(0L, Arrays.asList(1.0D, 2.0D));
        expectedCosts.put(1L, Arrays.asList(5.0D));
        expectedCosts.put(2L, Arrays.asList(7.0D, 3.0, 4.0));

        runQueryWithRowConsumer(cypher, row -> {
            Path path = (Path) row.get("path");

            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());
            List<Double> actualCosts = StreamSupport.stream(path.relationships().spliterator(), false)
                    .map(rel -> (double)rel.getProperty("cost")).collect(toList());

            long rowIndex = row.getNumber("index").longValue();
            assertEquals(expectedNodes.get(rowIndex), actualNodes);
            assertEquals(expectedCosts.get(rowIndex), actualCosts);
        });
    }

    @Test
    void shouldNotStoreWeightsOnVirtualPathIfIgnoringProperty() {
        String algoCall = GdsCypher.call()
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds.alpha.kShortestPaths")
            .streamMode()
            .addVariable("startNode", "a")
            .addVariable("endNode", "d")
            .addParameter("k", 42)
            .addParameter("path", true)
            .yields("index", "sourceNodeId", "targetNodeId", "nodeIds", "costs", "path");

        String cypher = String.format(
            "MATCH (a:Node{name:'a'}), (d:Node{name:'d'}) %s RETURN *",
            algoCall
        );

        Map<Long, List<Node>> expectedNodes = new HashMap<>();
        expectedNodes.put(0L, getNodes("a", "d"));
        expectedNodes.put(1L, getNodes("a", "b", "d"));
        expectedNodes.put(2L, getNodes("a", "e", "f", "d"));

        Map<Long, List<Double>> expectedCosts = new HashMap<>();
        expectedCosts.put(0L, Arrays.asList(1.0D, 2.0D));
        expectedCosts.put(1L, Arrays.asList(5.0D));
        expectedCosts.put(2L, Arrays.asList(7.0D, 3.0, 4.0));

        runQueryWithRowConsumer(cypher, row -> {
            Path path = (Path) row.get("path");

            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());

            long rowIndex = row.getNumber("index").longValue();
            assertEquals(expectedNodes.get(rowIndex), actualNodes);

            assertFalse(path.relationships().iterator().next().hasProperty("cost"));
        });
    }

    private List<Long> getNodeIds(String... nodes) {
        return runInTransaction(db, () ->
            Arrays.stream(nodes)
                .map(name -> db.findNode(Label.label("Node"), "name", name).getId())
                .collect(toList())
        );
    }
    private List<Node> getNodes(String... nodes) {
        return runInTransaction(db, () ->
            Arrays.stream(nodes)
                .map(name -> db.findNode(Label.label("Node"), "name", name))
                .collect(toList())
        );
    }
}
