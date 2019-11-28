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

package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.TestGraphLoader;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class GraphLoaderTest {

    public static final String DB_CYPHER =
            "CREATE" +
            "  (n1:Node1 {prop1: 1})" +
            ", (n2:Node2 {prop2: 2})" +
            ", (n3:Node3 {prop3: 3})" +
            ", (n1)-[:REL1 {prop1: 1}]->(n2)" +
            ", (n1)-[:REL2 {prop2: 2}]->(n3)" +
            ", (n2)-[:REL1 {prop3: 3, weight: 42}]->(n3)" +
            ", (n2)-[:REL3 {prop4: 4, weight: 1337}]->(n3)";

    private GraphDatabaseAPI db;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @AllGraphTypesTest
    void testAnyLabel(Class<? extends GraphFactory> graphFactory) {
        Graph graph = TestGraphLoader.from(db).buildGraph(graphFactory);
        assertGraphEquals(graph, fromGdl("(a)-->(b), (a)-->(c), (b)-->(c)"));
    }

    @AllGraphTypesTest
    void testWithLabel(Class<? extends GraphFactory> graphFactory) {
        Graph graph = TestGraphLoader.from(db).withLabel("Node1").buildGraph(graphFactory);
        assertGraphEquals(graph, fromGdl("()"));
    }

    @AllGraphTypesTest
    void testWithMultipleLabels(Class<? extends GraphFactory> graphFactory) {
        Graph  graph = TestGraphLoader.from(db).withLabel("Node1 | Node2").buildGraph(graphFactory);
        assertGraphEquals(graph, fromGdl("(a)-->(b)"));
    }

    @AllGraphTypesTest
    void testWithMultipleLabelsAndProperties(Class<? extends GraphFactory> graphFactory) {
        PropertyMappings properties = PropertyMappings.of(PropertyMapping.of("prop1", 42.0));
        PropertyMappings multipleProperties = PropertyMappings.of(
            PropertyMapping.of("prop1", 42.0),
            PropertyMapping.of("prop2", 42.0)
        );

        Graph graph = TestGraphLoader.from(db)
            .withLabel("Node1 | Node2")
            .withNodeProperties(properties)
            .buildGraph(graphFactory);
        assertGraphEquals(graph, fromGdl("(a {prop1: 1.0})-->(b {prop1: 42.0})"));

        graph = TestGraphLoader.from(db)
            .withLabel("Node1 | Node2")
            .withNodeProperties(multipleProperties)
            .buildGraph(graphFactory);
        assertGraphEquals(graph, fromGdl("(a {prop1: 1.0, prop2: 42.0})-->(b {prop1: 42.0, prop2: 2.0})"));
    }

    @AllGraphTypesTest
    void testAnyRelation(Class<? extends GraphFactory> graphFactory) {
        Graph graph = TestGraphLoader.from(db).buildGraph(graphFactory);
        assertGraphEquals(graph, fromGdl("(a)-->(b), (a)-->(c), (b)-->(c)"));
    }

    @AllGraphTypesTest
    void testWithBothWeightedRelationship(Class<? extends GraphFactory> graphFactory) {
        Graph graph = TestGraphLoader.from(db)
            .withRelationshipType("REL3")
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .withDirection(Direction.OUTGOING)
            .buildGraph(graphFactory);

        assertGraphEquals(graph, fromGdl("(), ()-[{w:1337}]->()"));
    }

    @AllGraphTypesTest
    void testWithOutgoingRelationship(Class<? extends GraphFactory> graphFactory) {
        Graph graph = TestGraphLoader.from(db)
            .withRelationshipType("REL3")
            .withDirection(Direction.OUTGOING)
            .buildGraph(graphFactory);
        assertGraphEquals(graph, fromGdl("(), ()-->()"));
    }

    @AllGraphTypesTest
    void testWithNodeProperties(Class<? extends GraphFactory> graphFactory) {
        PropertyMappings nodePropertyMappings = PropertyMappings.of(
                PropertyMapping.of("prop1", "prop1", 0D),
                PropertyMapping.of("prop2", "prop2", 0D),
                PropertyMapping.of("prop3", "prop3", 0D));

        Graph graph = TestGraphLoader
            .from(db)
            .withNodeProperties(nodePropertyMappings)
            .buildGraph(graphFactory);

        assertGraphEquals(graph, fromGdl("(a {prop1: 1, prop2: 0, prop3: 0})" +
                                         "(b {prop1: 0, prop2: 2, prop3: 0})" +
                                         "(c {prop1: 0, prop2: 0, prop3: 3})" +
                                         "(a)-->(b), (a)-->(c), (b)-->(c)"));
    }

    @AllGraphTypesTest
    void testWithRelationshipProperty(Class<? extends GraphFactory> graphFactory) {
        Graph graph = TestGraphLoader.from(db)
            .withRelationshipProperties(PropertyMapping.of("prop1", 1337.42))
            .buildGraph(graphFactory);
        assertGraphEquals(graph, fromGdl("(a)-[{w: 1}]->(b), (a)-[{w: 1337.42D}]->(c), (b)-[{w: 1337.42D}]->(c)"));
    }

    @AllGraphTypesWithoutCypherTest
    void stopsImportingWhenTransactionHasBeenTerminated(Class<? extends GraphFactory> graphFactory) {
        TerminationFlag terminationFlag = () -> false;
        TransactionTerminatedException exception = assertThrows(
                TransactionTerminatedException.class,
                () -> new GraphLoader(db)
                        .withTerminationFlag(terminationFlag)
                        .load(graphFactory)
        );
        assertEquals(Status.Transaction.Terminated, exception.status());
    }
}
