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
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.impl.results.CentralityResult;
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
public final class PageRankWikiTest {

    private Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"}
        );
    }

    private static final String DB_CYPHER = "" +
            "CREATE (a:Node {name:\"a\"})\n" +
            "CREATE (b:Node {name:\"b\"})\n" +
            "CREATE (c:Node {name:\"c\"})\n" +
            "CREATE (d:Node {name:\"d\"})\n" +
            "CREATE (e:Node {name:\"e\"})\n" +
            "CREATE (f:Node {name:\"f\"})\n" +
            "CREATE (g:Node {name:\"g\"})\n" +
            "CREATE (h:Node {name:\"h\"})\n" +
            "CREATE (i:Node {name:\"i\"})\n" +
            "CREATE (j:Node {name:\"j\"})\n" +
            "CREATE (k:Node {name:\"k\"})\n" +
            "CREATE\n" +
            // a (dangling node)
            // b
            "  (b)-[:TYPE]->(c),\n" +
            // c
            "  (c)-[:TYPE]->(b),\n" +
            // d
            "  (d)-[:TYPE]->(a),\n" +
            "  (d)-[:TYPE]->(b),\n" +
            // e
            "  (e)-[:TYPE]->(b),\n" +
            "  (e)-[:TYPE]->(d),\n" +
            "  (e)-[:TYPE]->(f),\n" +
            // f
            "  (f)-[:TYPE]->(b),\n" +
            "  (f)-[:TYPE]->(e),\n" +
            // g
            "  (g)-[:TYPE]->(b),\n" +
            "  (g)-[:TYPE]->(e),\n" +
            // h
            "  (h)-[:TYPE]->(b),\n" +
            "  (h)-[:TYPE]->(e),\n" +
            // i
            "  (i)-[:TYPE]->(b),\n" +
            "  (i)-[:TYPE]->(e),\n" +
            // j
            "  (j)-[:TYPE]->(e),\n" +
            // k
            "  (k)-[:TYPE]->(e)\n";

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        db.shutdown();
    }

    public PageRankWikiTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void test() throws Exception {
        final Label label = Label.label("Node");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.3040965);
            expected.put(db.findNode(label, "name", "b").getId(), 3.5658695);
            expected.put(db.findNode(label, "name", "c").getId(), 3.180981);
            expected.put(db.findNode(label, "name", "d").getId(), 0.3625935);
            expected.put(db.findNode(label, "name", "e").getId(), 0.7503465);
            expected.put(db.findNode(label, "name", "f").getId(), 0.3625935);
            expected.put(db.findNode(label, "name", "g").getId(), 0.15);
            expected.put(db.findNode(label, "name", "h").getId(), 0.15);
            expected.put(db.findNode(label, "name", "i").getId(), 0.15);
            expected.put(db.findNode(label, "name", "j").getId(), 0.15);
            expected.put(db.findNode(label, "name", "k").getId(), 0.15);
            tx.close();
        }

        final Graph graph = new GraphLoader(db)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .load(graphImpl);

        final CentralityResult rankResult = PageRankFactory
                .of(graph, 0.85, LongStream.empty())
                .compute(40)
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
