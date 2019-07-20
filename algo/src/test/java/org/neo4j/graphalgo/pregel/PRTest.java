package org.neo4j.graphalgo.pregel;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.pregel.pagerank.PRComputation;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

public class PRTest {

    private static final String RANK_PROPERTY = "rank";
    private static final String MESSAGE_PROPERTY = "message";

    // https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.jpg
    private static final String TEST_GRAPH = "" +
                                             "CREATE (a:Node { name: 'a', rank: 1.0 })\n" +
                                             "CREATE (b:Node { name: 'b', rank: 1.0 })\n" +
                                             "CREATE (c:Node { name: 'c', rank: 1.0 })\n" +
                                             "CREATE (d:Node { name: 'd', rank: 1.0 })\n" +
                                             "CREATE (e:Node { name: 'e', rank: 1.0 })\n" +
                                             "CREATE (f:Node { name: 'f', rank: 1.0 })\n" +
                                             "CREATE (g:Node { name: 'g', rank: 1.0 })\n" +
                                             "CREATE (h:Node { name: 'h', rank: 1.0 })\n" +
                                             "CREATE (i:Node { name: 'i', rank: 1.0 })\n" +
                                             "CREATE (j:Node { name: 'j', rank: 1.0 })\n" +
                                             "CREATE (k:Node { name: 'k', rank: 1.0 })\n" +
                                             "CREATE\n" +
                                             "  (b)-[:REL { message: 1.0 }]->(c),\n" +
                                             "  (c)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (d)-[:REL { message: 1.0 }]->(a),\n" +
                                             "  (d)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (e)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (e)-[:REL { message: 1.0 }]->(d),\n" +
                                             "  (e)-[:REL { message: 1.0 }]->(f),\n" +
                                             "  (f)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (f)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (g)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (g)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (h)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (h)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (i)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (i)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (j)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (k)-[:REL { message: 1.0 }]->(e)\n";


    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setup() {
        DB.execute(TEST_GRAPH);
    }

    @AfterClass
    public static void shutdown() {
        DB.shutdown();
    }

    private Graph graph;

    public PRTest() {

        PropertyMapping propertyMapping = new PropertyMapping(RANK_PROPERTY, RANK_PROPERTY, 1.0);

        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                // The following options need to be default for Pregel
                .withDirection(Direction.BOTH)
                .withOptionalNodeProperties(propertyMapping)
                .withOptionalRelationshipWeightsFromProperty(MESSAGE_PROPERTY, 1.0)
                .load(HugeGraphFactory.class);
    }

    @Test
    public void runPR() {
        HugeWeightMapping nodeProperties = graph.nodeProperties(RANK_PROPERTY);

        int batchSize = 10;
        int maxIterations = 50;
        float jumpProbablity = 0.15f;
        float dampingFactor = 0.85f;

        Pregel pregelJob = new Pregel(
                graph,
                nodeProperties,
                new PRComputation(graph.nodeCount(), jumpProbablity, dampingFactor),
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                ProgressLogger.NULL_LOGGER);

        int ranIterations = pregelJob.run(maxIterations);

        System.out.printf("Ran %d iterations.%n", ranIterations);

        for (int i = 0; i < graph.nodeCount(); i++) {
            System.out.println(String.format("nodeId: %d, rank: %.4f", i, nodeProperties.get(i)));
        }
    }
}
