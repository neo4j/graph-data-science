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
import org.neo4j.graphalgo.pregel.components.SCComputation;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

public class SCCTest {

    private static final String COMPONENT_PROPERTY = "component";
    private static final String MESSAGE_PROPERTY = "message";

    private static final String TEST_GRAPH =
            "CREATE (nA { component: 1 })\n" +
            "CREATE (nB { component: 1 })\n" +
            "CREATE (nC { component: 1 })\n" +
            "CREATE (nD { component: 1 })\n" +
            "CREATE (nE { component: 1 })\n" +
            "CREATE (nF { component: 1 })\n" +
            "CREATE (nG { component: 1 })\n" +
            "CREATE (nH { component: 1 })\n" +
            "CREATE (nI { component: 1 })\n" +
            "CREATE (nJ { component: 1 })\n" + // {J}
            "CREATE\n" +
            // {A, B, C, D}
            "  (nA)-[:TYPE { message: 1 }]->(nB),\n" +
            "  (nB)-[:TYPE { message: 1 }]->(nC),\n" +
            "  (nC)-[:TYPE { message: 1 }]->(nD),\n" +
            "  (nD)-[:TYPE { message: 1 }]->(nA),\n" +
            // {E, F, G}
            "  (nE)-[:TYPE { message: 1 }]->(nF),\n" +
            "  (nF)-[:TYPE { message: 1 }]->(nG),\n" +
            "  (nG)-[:TYPE { message: 1 }]->(nE),\n" +
            // {H, I}
            "  (nI)-[:TYPE { message: 1 }]->(nH),\n" +
            "  (nH)-[:TYPE { message: 1 }]->(nI)";

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

    public SCCTest() {

        PropertyMapping propertyMapping = new PropertyMapping(COMPONENT_PROPERTY, COMPONENT_PROPERTY, 1);

        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                // The following options need to be default for Pregel
                .withDirection(Direction.BOTH)
                .withOptionalNodeProperties(propertyMapping)
                .withOptionalRelationshipWeightsFromProperty(MESSAGE_PROPERTY, 1)
                .load(HugeGraphFactory.class);
    }

    @Test
    public void runSCC() {
        HugeWeightMapping nodeProperties = graph.nodeProperties(COMPONENT_PROPERTY);

        int batchSize = 10;
        int maxIterations = 10;

        Pregel pregelJob = new Pregel(graph,
                nodeProperties,
                new SCComputation(),
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                ProgressLogger.NULL_LOGGER);

        int ranIterations = pregelJob.run(maxIterations);

        System.out.printf("Ran %d iterations.%n", ranIterations);

        for (int i = 0; i < graph.nodeCount(); i++) {
            System.out.println(String.format("nodeId: %d, componentId: %d", i, (long) nodeProperties.get(i)));
        }
    }
}
