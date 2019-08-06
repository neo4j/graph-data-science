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
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(Parameterized.class)
public final class PageRankTest {

    static PageRank.Config DEFAULT_CONFIG = new PageRank.Config(40, 0.85, PageRank.DEFAULT_TOLERANCE);

    private Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{HeavyCypherGraphFactory.class, "HeavyCypherGraphFactory"},
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
    public static void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() {
        if (db != null) db.shutdown();
    }

    public PageRankTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void testOnOutgoingRelationships() {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.243007);
            expected.put(db.findNode(label, "name", "b").getId(), 1.9183995);
            expected.put(db.findNode(label, "name", "c").getId(), 1.7806315);
            expected.put(db.findNode(label, "name", "d").getId(), 0.21885);
            expected.put(db.findNode(label, "name", "e").getId(), 0.243007);
            expected.put(db.findNode(label, "name", "f").getId(), 0.21885);
            expected.put(db.findNode(label, "name", "g").getId(), 0.15);
            expected.put(db.findNode(label, "name", "h").getId(), 0.15);
            expected.put(db.findNode(label, "name", "i").getId(), 0.15);
            expected.put(db.findNode(label, "name", "j").getId(), 0.15);
            tx.close();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
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

        final CentralityResult rankResult = PageRankAlgorithmType.NON_WEIGHTED
                .create(graph, DEFAULT_CONFIG, LongStream.empty())
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

    @Test
    public void testOnIncomingRelationships() {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.15);
            expected.put(db.findNode(label, "name", "b").getId(), 0.3386727);
            expected.put(db.findNode(label, "name", "c").getId(), 0.2219679);
            expected.put(db.findNode(label, "name", "d").getId(), 0.3494679);
            expected.put(db.findNode(label, "name", "e").getId(), 2.5463981);
            expected.put(db.findNode(label, "name", "f").getId(), 2.3858317);
            expected.put(db.findNode(label, "name", "g").getId(), 0.15);
            expected.put(db.findNode(label, "name", "h").getId(), 0.15);
            expected.put(db.findNode(label, "name", "i").getId(), 0.15);
            expected.put(db.findNode(label, "name", "j").getId(), 0.15);
            tx.close();
        }

        final Graph graph;
        final CentralityResult rankResult;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)<-[:TYPE1]-(m:Label1) RETURN id(n) as source,id(m) as target")
                    .load(graphImpl);

            rankResult = PageRankAlgorithmType.NON_WEIGHTED
                    .create(graph, DEFAULT_CONFIG, LongStream.empty())
                    .compute()
                    .result();
        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.INCOMING)
                    .load(graphImpl);

            rankResult = PageRankAlgorithmType.NON_WEIGHTED
                    .create(graph, DEFAULT_CONFIG, LongStream.empty())
                    .compute()
                    .result();
        }

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

    @Test
    public void correctPartitionBoundariesForAllNodes() {
        final Label label = Label.label("Label1");
        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
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

        // explicitly list all source nodes to prevent the 'we got everything' optimization
        PageRank algorithm = PageRankAlgorithmType.NON_WEIGHTED
                .create(
                        graph,
                        null,
                        1,
                        1,
                        DEFAULT_CONFIG,
                        LongStream.range(0L, graph.nodeCount()),
                        AllocationTracker.EMPTY)
                .compute();
        // should not throw
    }

    @Test
    public void shouldComputeMemoryEstimation1Thread() {
        long nodeCount = 100_000L;
        int concurrency = 1;
        assertMemoryEstimation(nodeCount, concurrency);
    }

    @Test
    public void shouldComputeMemoryEstimation4Threads() {
        long nodeCount = 100_000L;
        int concurrency = 4;
        assertMemoryEstimation(nodeCount, concurrency);
    }

    @Test
    public void shouldComputeMemoryEstimation42Threads() {
        long nodeCount = 100_000L;
        int concurrency = 42;
        assertMemoryEstimation(nodeCount, concurrency);
    }

    private void assertMemoryEstimation(final long nodeCount, final int concurrency) {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(nodeCount).build();

        final PageRankFactory pageRank = new PageRankFactory(DEFAULT_CONFIG);

        long partitionSize = BitUtil.ceilDiv(nodeCount, concurrency);
        final MemoryRange actual = pageRank.memoryEstimation().estimate(dimensions, concurrency).memoryUsage();
        final MemoryRange expected = MemoryRange.of(
                96L /* PageRank.class */ +
                32L /* ComputeSteps.class */ +
                BitUtil.align(16 + concurrency * 4, 8) /* scores[] wrapper */ +
                BitUtil.align(16 + concurrency * 8, 8) /* starts[] */ +
                BitUtil.align(16 + concurrency * 8, 8) /* lengths[] */ +
                BitUtil.align(16 + concurrency * 4, 8) /* list of computeSteps */ +
                        /* ComputeStep */
                concurrency * (
                        120L /* NonWeightedComputeStep.class */ +
                        BitUtil.align(16 + concurrency * 4, 8) /* nextScores[] wrapper */ +
                        concurrency * BitUtil.align(16 + partitionSize * 4, 8) /* inner nextScores[][] */ +
                        BitUtil.align(16 + partitionSize * 8, 8) /* pageRank[] */ +
                        BitUtil.align(16 + partitionSize * 8, 8) /* deltas[] */
                )
        );
        assertEquals(expected, actual);
    }
}
