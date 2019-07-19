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
import org.neo4j.graphalgo.pregel.components.WCComputation;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

public class WCCTest {

    private static final String COMPONENT_PROPERTY = "component";
    private static final String MESSAGE_PROPERTY = "message";

    private static final String TEST_GRAPH =
            "CREATE (nA { component: -1 })\n" +
            "CREATE (nB { component: -1 })\n" +
            "CREATE (nC { component: -1 })\n" +
            "CREATE (nD { component: -1 })\n" +
            "CREATE (nE { component: -1 })\n" +
            "CREATE (nF { component: -1 })\n" +
            "CREATE (nG { component: -1 })\n" +
            "CREATE (nH { component: -1 })\n" +
            "CREATE (nI { component: -1 })\n" +
            "CREATE (nJ { component: -1 })\n" + // {J}
            "CREATE\n" +
            // {A, B, C, D}
            "  (nA)-[:TYPE { message: -2 }]->(nB),\n" +
            "  (nB)-[:TYPE { message: -2 }]->(nC),\n" +
            "  (nC)-[:TYPE { message: -2 }]->(nD),\n" +
            // {E, F, G}
            "  (nE)-[:TYPE { message: -2 }]->(nF),\n" +
            "  (nF)-[:TYPE { message: -2 }]->(nG),\n" +
            // {H, I}
            "  (nH)-[:TYPE { message: -2 }]->(nI)";

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

    public WCCTest() {

        PropertyMapping propertyMapping = new PropertyMapping(COMPONENT_PROPERTY, COMPONENT_PROPERTY, -1);

        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                // The following options need to be default for Pregel
                .withDirection(Direction.BOTH)
                .withOptionalNodeProperties(propertyMapping)
                .withOptionalRelationshipWeightsFromProperty(MESSAGE_PROPERTY, -2)
                .load(HugeGraphFactory.class);
    }

    @Test
    public void runScc() {
        HugeWeightMapping nodeProperties = graph.nodeProperties(COMPONENT_PROPERTY);

        int batchSize = 10;

        Pregel pregelJob = new Pregel(graph,
                nodeProperties,
                new WCComputation(),
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                ProgressLogger.NULL_LOGGER);

        pregelJob.run(10);

        for (int i = 0; i < graph.nodeCount(); i++) {
            System.out.println(String.format("nodeId: %d, componentId: %d", i, (long) nodeProperties.get(i)));
        }
    }
}
