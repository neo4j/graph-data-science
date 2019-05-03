package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.junit.*;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.neo4j.graphalgo.impl.labelprop.LabelPropagation.PARTITION_TYPE;
import static org.neo4j.graphalgo.impl.labelprop.LabelPropagation.WEIGHT_TYPE;

@RunWith(Parameterized.class)
public final class LabelPropagationTest {

    private static final String GRAPH =
            "CREATE (nAlice:User {id:'Alice',label:2})\n" +
                    ",(nBridget:User {id:'Bridget',label:3})\n" +
                    ",(nCharles:User {id:'Charles',label:4})\n" +
                    ",(nDoug:User {id:'Doug',label:3})\n" +
                    ",(nMark:User {id:'Mark',label: 4})\n" +
                    ",(nMichael:User {id:'Michael',label:2})\n" +
                    "CREATE (nAlice)-[:FOLLOW]->(nBridget)\n" +
                    ",(nAlice)-[:FOLLOW]->(nCharles)\n" +
                    ",(nMark)-[:FOLLOW]->(nDoug)\n" +
                    ",(nBridget)-[:FOLLOW]->(nMichael)\n" +
                    ",(nDoug)-[:FOLLOW]->(nMark)\n" +
                    ",(nMichael)-[:FOLLOW]->(nAlice)\n" +
                    ",(nAlice)-[:FOLLOW]->(nMichael)\n" +
                    ",(nBridget)-[:FOLLOW]->(nAlice)\n" +
                    ",(nMichael)-[:FOLLOW]->(nBridget)\n" +
                    ",(nCharles)-[:FOLLOW]->(nDoug)";

    @Parameterized.Parameters(name = "graph={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class},
                new Object[]{HeavyCypherGraphFactory.class},
                new Object[]{HugeGraphFactory.class}
        );
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        DB.execute(GRAPH).close();
    }

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private final Class<? extends GraphFactory> graphImpl;
    private Graph graph;

    public LabelPropagationTest(Class<? extends GraphFactory> graphImpl) {
        this.graphImpl = graphImpl;
    }

    @Before
    public void setup() {
        GraphLoader graphLoader = new GraphLoader(DB, Pools.DEFAULT)
                .withRelationshipWeightsFromProperty("weight", 1.0)
                .withOptionalNodeProperties(
                        PropertyMapping.of(WEIGHT_TYPE, WEIGHT_TYPE, 1.0),
                        PropertyMapping.of(PARTITION_TYPE, PARTITION_TYPE, 0.0)
                )
                .withDirection(Direction.BOTH)
                .withConcurrency(Pools.DEFAULT_CONCURRENCY);

        if (graphImpl == HeavyCypherGraphFactory.class) {
            graphLoader
                    .withLabel("MATCH (u:User) RETURN id(u) as id")
                    .withRelationshipType("MATCH (u1:User)-[rel:FOLLOW]->(u2:User) \n" +
                            "RETURN id(u1) as source,id(u2) as target")
                    .withName("cypher");
        } else {
            graphLoader
                    .withLabel("User")
                    .withRelationshipType("FOLLOW")
                    .withName(graphImpl.getSimpleName());
        }
        graph = graphLoader.load(graphImpl);
    }

    @Test
    public void testSingleThreadClustering() {
        testClustering(100);
    }

    @Test
    public void testMultiThreadClustering() {
        testClustering(2);
    }

    @Test
    public void testHugeSingleThreadClustering() {
        testClustering(100);
    }

    @Test
    public void testHugeMultiThreadClustering() {
        testClustering(2);
    }

    private void testClustering(int batchSize) {
        testClustering(new LabelPropagation(
                graph,
                graph,
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY
        ));
    }

    // possible bad seed: -2300107887844480632
    private void testClustering(LabelPropagation lp) {
        Long seed = Long.getLong("tests.seed");
        if (seed != null) {
            lp.compute(Direction.OUTGOING, 10L, seed);
        } else {
            lp.compute(Direction.OUTGOING, 10L);
        }
        LabelPropagation.Labels labels = lp.labels();
        assertNotNull(labels);
        IntObjectMap<IntArrayList> cluster = groupByPartitionInt(labels);
        assertNotNull(cluster);

        // It could happen that the labels for Charles, Doug, and Mark oscillate,
        // i.e they assign each others' label in every iteration and the graph won't converge.
        // LPA runs asynchronous and shuffles the order of iteration a bit to try
        // to minimize the oscillations, but it cannot be guaranteed that
        // it will never happen. It's RNG after all: http://dilbert.com/strip/2001-10-25
        if (lp.didConverge()) {
            assertTrue("expected at least 2 iterations, got " + lp.ranIterations(), 2L <= lp.ranIterations());
            assertEquals(2L, (long) cluster.size());
            for (IntObjectCursor<IntArrayList> cursor : cluster) {
                int[] ids = cursor.value.toArray();
                Arrays.sort(ids);
                if (cursor.key == 0 || cursor.key == 1 || cursor.key == 5) {
                    assertArrayEquals(new int[]{0, 1, 5}, ids);
                } else if (cursor.key == 2) {
                    if (ids[0] == 0) {
                        assertArrayEquals(new int[]{0, 1, 5}, ids);
                    } else {
                        assertArrayEquals(new int[]{2, 3, 4}, ids);
                    }
                }
            }
        } else {
            assertEquals((long) 10, lp.ranIterations());
            System.out.println("non-converged cluster = " + cluster);
            IntArrayList cluster5 = cluster.get(5);
            assertNotNull(cluster5);
            int[] ids = cluster5.toArray();
            Arrays.sort(ids);
            assertArrayEquals(new int[]{0, 1, 5}, ids);
        }
    }

    private static IntObjectMap<IntArrayList> groupByPartitionInt( LabelPropagation.Labels labels) {
        if (labels == null) {
            return null;
        }
        IntObjectMap<IntArrayList> cluster = new IntObjectHashMap<>();
        for (int node = 0, l = Math.toIntExact(labels.size()); node < l; node++) {
            int key = Math.toIntExact(labels.labelFor(node));
            IntArrayList ids = cluster.get(key);
            if (ids == null) {
                ids = new IntArrayList();
                cluster.put(key, ids);
            }
            ids.add(node);
        }

        return cluster;
    }
}
