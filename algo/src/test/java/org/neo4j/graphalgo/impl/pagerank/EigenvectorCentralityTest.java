/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.pagerank;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class EigenvectorCentralityTest {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (_:Label0 {name: '_'})" +
            ", (a:Label1 {name: 'a'})" +
            ", (b:Label1 {name: 'b'})" +
            ", (c:Label1 {name: 'c'})" +
            ", (d:Label1 {name: 'd'})" +
            ", (e:Label1 {name: 'e'})" +
            ", (f:Label1 {name: 'f'})" +
            ", (g:Label1 {name: 'g'})" +
            ", (h:Label1 {name: 'h'})" +
            ", (i:Label1 {name: 'i'})" +
            ", (j:Label1 {name: 'j'})" +
            ", (k:Label2 {name: 'k'})" +
            ", (l:Label2 {name: 'l'})" +
            ", (m:Label2 {name: 'm'})" +
            ", (n:Label2 {name: 'n'})" +
            ", (o:Label2 {name: 'o'})" +
            ", (p:Label2 {name: 'p'})" +
            ", (q:Label2 {name: 'q'})" +
            ", (r:Label2 {name: 'r'})" +
            ", (s:Label2 {name: 's'})" +
            ", (t:Label2 {name: 't'})" +

            ",  (b)-[:TYPE1]->(c)" +
            ",  (c)-[:TYPE1]->(b)" +

            ",  (d)-[:TYPE1]->(a)" +
            ",  (d)-[:TYPE1]->(b)" +

            ",  (e)-[:TYPE1]->(b)" +
            ",  (e)-[:TYPE1]->(d)" +
            ",  (e)-[:TYPE1]->(f)" +

            ",  (f)-[:TYPE1]->(b)" +
            ",  (f)-[:TYPE1]->(e)" +

            ",  (g)-[:TYPE2]->(b)" +
            ",  (g)-[:TYPE2]->(e)" +
            ",  (h)-[:TYPE2]->(b)" +
            ",  (h)-[:TYPE2]->(e)" +
            ",  (i)-[:TYPE2]->(b)" +
            ",  (i)-[:TYPE2]->(e)" +
            ",  (j)-[:TYPE2]->(e)" +
            ",  (k)-[:TYPE2]->(e)";

    private static final PageRank.Config DEFAULT_EIGENVECTOR_CONFIG = new PageRank.Config(40, 1, PageRank.DEFAULT_TOLERANCE);
    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setupGraphDb() {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.execute(DB_CYPHER);
    }

    @AfterAll
    static void shutdownGraphDb() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphTypesTest
    void test(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = DB.beginTx()) {
            expected.put(DB.findNode(label, "name", "a").getId(), 1.762540000000000);
            expected.put(DB.findNode(label, "name", "b").getId(), 31.156790000000008);
            expected.put(DB.findNode(label, "name", "c").getId(), 28.694439999999993);
            expected.put(DB.findNode(label, "name", "d").getId(), 1.7625400000000004);
            expected.put(DB.findNode(label, "name", "e").getId(), 1.7625400000000004);
            expected.put(DB.findNode(label, "name", "f").getId(), 1.7625400000000004);
            expected.put(DB.findNode(label, "name", "g").getId(), 0.1);
            expected.put(DB.findNode(label, "name", "h").getId(), 0.1);
            expected.put(DB.findNode(label, "name", "i").getId(), 0.1);
            expected.put(DB.findNode(label, "name", "j").getId(), 0.1);
            tx.success();
        }

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(DB)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target")
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(DB)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .load(graphFactory);
        }

        final CentralityResult rankResult = PageRankAlgorithmType.EIGENVECTOR_CENTRALITY
                .create(graph, DEFAULT_EIGENVECTOR_CONFIG, LongStream.empty())
                .compute()
                .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2,
                    "Node#" + nodeId
            );
        });
    }
}
