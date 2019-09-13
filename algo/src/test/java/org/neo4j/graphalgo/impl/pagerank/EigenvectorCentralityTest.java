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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class EigenvectorCentralityTest {

    static PageRank.Config DEFAULT_EIGENVECTOR_CONFIG = new PageRank.Config(40, 1, PageRank.DEFAULT_TOLERANCE);

    private Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{CypherGraphFactory.class, "CypherGraphFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"}
        );
    }

    private static final String DB_CYPHER = "" +
            "CREATE (_:Label0 {name:\"_\"})\n" +
            "CREATE (a:Label1 {name:\"a\"})\n" +
            "CREATE (b:Label1 {name:\"b\"})\n" +
            "CREATE (c:Label1 {name:\"c\"})\n" +
            "CREATE (d:Label1 {name:\"d\"})\n" +
            "CREATE (e:Label1 {name:\"e\"})\n" +
            "CREATE (f:Label1 {name:\"f\"})\n" +
            "CREATE (g:Label1 {name:\"g\"})\n" +
            "CREATE (h:Label1 {name:\"h\"})\n" +
            "CREATE (i:Label1 {name:\"i\"})\n" +
            "CREATE (j:Label1 {name:\"j\"})\n" +
            "CREATE (k:Label2 {name:\"k\"})\n" +
            "CREATE (l:Label2 {name:\"l\"})\n" +
            "CREATE (m:Label2 {name:\"m\"})\n" +
            "CREATE (n:Label2 {name:\"n\"})\n" +
            "CREATE (o:Label2 {name:\"o\"})\n" +
            "CREATE (p:Label2 {name:\"p\"})\n" +
            "CREATE (q:Label2 {name:\"q\"})\n" +
            "CREATE (r:Label2 {name:\"r\"})\n" +
            "CREATE (s:Label2 {name:\"s\"})\n" +
            "CREATE (t:Label2 {name:\"t\"})\n" +
            "CREATE\n" +
            "  (b)-[:TYPE1]->(c),\n" +

            "  (c)-[:TYPE1]->(b),\n" +

            "  (d)-[:TYPE1]->(a),\n" +
            "  (d)-[:TYPE1]->(b),\n" +

            "  (e)-[:TYPE1]->(b),\n" +
            "  (e)-[:TYPE1]->(d),\n" +
            "  (e)-[:TYPE1]->(f),\n" +

            "  (f)-[:TYPE1]->(b),\n" +
            "  (f)-[:TYPE1]->(e),\n" +

            "  (g)-[:TYPE2]->(b),\n" +
            "  (g)-[:TYPE2]->(e),\n" +
            "  (h)-[:TYPE2]->(b),\n" +
            "  (h)-[:TYPE2]->(e),\n" +
            "  (i)-[:TYPE2]->(b),\n" +
            "  (i)-[:TYPE2]->(e),\n" +
            "  (j)-[:TYPE2]->(e),\n" +
            "  (k)-[:TYPE2]->(e)\n";

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraphDb() {
        if (db!=null) db.shutdown();
    }

    public EigenvectorCentralityTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void test() {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 1.762540000000000);
            expected.put(db.findNode(label, "name", "b").getId(), 31.156790000000008);
            expected.put(db.findNode(label, "name", "c").getId(), 28.694439999999993);
            expected.put(db.findNode(label, "name", "d").getId(), 1.7625400000000004);
            expected.put(db.findNode(label, "name", "e").getId(), 1.7625400000000004);
            expected.put(db.findNode(label, "name", "f").getId(), 1.7625400000000004);
            expected.put(db.findNode(label, "name", "g").getId(), 0.1);
            expected.put(db.findNode(label, "name", "h").getId(), 0.1);
            expected.put(db.findNode(label, "name", "i").getId(), 0.1);
            expected.put(db.findNode(label, "name", "j").getId(), 0.1);
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target")
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);
        }

        final CentralityResult rankResult = PageRankAlgorithmType.EIGENVECTOR_CENTRALITY
                .create(graph, DEFAULT_EIGENVECTOR_CONFIG, LongStream.empty())
                .compute()
                .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2
            );
        });
    }
}
