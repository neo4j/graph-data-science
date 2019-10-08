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
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

public class GraphLoaderTest {

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
        Graph graph = initLoader(graphFactory);
        assertGraphEquals(graph, fromGdl("(a)-->(b), (a)-->(c), (b)-->(c)"));
    }

    @AllGraphTypesTest
    void testWithLabel(Class<? extends GraphFactory> graphFactory) {
        Graph graph;
        try (Transaction tx = db.beginTx()) {
            graph = initLoader(graphFactory, "Node1", null).load(graphFactory);
        }
        assertGraphEquals(graph, fromGdl("()"));
    }

    @AllGraphTypesTest
    void testAnyRelation(Class<? extends GraphFactory> graphFactory) {
        Graph graph = initLoader(graphFactory);
        assertGraphEquals(graph, fromGdl("(a)-->(b), (a)-->(c), (b)-->(c)"));
    }

    @AllGraphTypesTest
    void testWithBothWeightedRelationship(Class<? extends GraphFactory> graphFactory) {
        PropertyMappings relPropertyMappings = PropertyMappings.of(PropertyMapping.of("weight", 1.0));

        Graph graph;
        try (Transaction tx = db.beginTx()) {
            graph = initLoader(
                    graphFactory,
                    Optional.empty(),
                    Optional.of("REL3"),
                    PropertyMappings.EMPTY,
                    relPropertyMappings)
                    .withDirection(Direction.OUTGOING)
                    .load(graphFactory);
        }
        assertGraphEquals(graph, fromGdl("(), ()-[{w:1337}]->()"));
    }

    @AllGraphTypesTest
    void testWithOutgoingRelationship(Class<? extends GraphFactory> graphFactory) {
        Graph graph;
        try (Transaction tx = db.beginTx()) {
            graph = initLoader(graphFactory, null, "REL3")
                    .withDirection(Direction.OUTGOING)
                    .load(graphFactory);
        }
        assertGraphEquals(graph, fromGdl("(), ()-->()"));
    }

    @AllGraphTypesTest
    void testWithNodeProperties(Class<? extends GraphFactory> graphFactory) {
        PropertyMappings nodePropertyMappings = PropertyMappings.of(
                PropertyMapping.of("prop1", "prop1", 0D),
                PropertyMapping.of("prop2", "prop2", 0D),
                PropertyMapping.of("prop3", "prop3", 0D));

        Graph graph;
        try (Transaction tx = db.beginTx()) {
            graph = initLoader(
                    graphFactory,
                    Optional.empty(),
                    Optional.empty(),
                    nodePropertyMappings,
                    PropertyMappings.EMPTY)
                    .load(graphFactory);
        }
        assertGraphEquals(graph, fromGdl("(a {prop1: 1, prop2: 0, prop3: 0})" +
                                         "(b {prop1: 0, prop2: 2, prop3: 0})" +
                                         "(c {prop1: 0, prop2: 0, prop3: 3})" +
                                         "(a)-->(b), (a)-->(c), (b)-->(c)"));
    }

    @AllGraphTypesTest
    void testWithRelationshipProperty(Class<? extends GraphFactory> graphFactory) {
        PropertyMappings relPropertyMappings = PropertyMappings.of(PropertyMapping.of("prop1", 1337.42));
        Graph graph;
        try (Transaction tx = db.beginTx()) {
            graph = initLoader(
                    graphFactory,
                    Optional.empty(),
                    Optional.empty(),
                    PropertyMappings.EMPTY,
                    relPropertyMappings).load(graphFactory);
        }
        assertGraphEquals(graph, fromGdl("(a)-[{w: 1}]->(b), (a)-[{w: 1337.42D}]->(c), (b)-[{w: 1337.42D}]->(c)"));
    }

    @AllGraphTypesWithoutCypherTest
    void stopsImportingWhenTransactionHasBeenTerminated(Class<? extends GraphFactory> graphFactory) {
        TerminationFlag terminationFlag = () -> false;
        TransactionTerminatedException exception = assertThrows(
                TransactionTerminatedException.class,
                () -> {
                    new GraphLoader(db)
                            .withTerminationFlag(terminationFlag)
                            .load(graphFactory);
                });
        assertEquals(Status.Transaction.Terminated, exception.status());
    }

    private Graph initLoader(Class<? extends GraphFactory> graphFactory) {
        try (Transaction tx = db.beginTx()) {
            return initLoader(graphFactory, null, null).load(graphFactory);
        }
    }

    private GraphLoader initLoader(Class<? extends GraphFactory> graphFactory, String label, String relType) {
        return initLoader(
                graphFactory,
                label != null ? Optional.of(label) : Optional.empty(),
                relType != null ? Optional.of(relType) : Optional.empty(),
                PropertyMappings.EMPTY,
                PropertyMappings.EMPTY);
    }

    private GraphLoader initLoader(
            Class<? extends GraphFactory> graphFactory,
            Optional<String> maybeLabel,
            Optional<String> maybeRelType,
            PropertyMappings nodeProperties,
            PropertyMappings relProperties) {

        GraphLoader graphLoader = new GraphLoader(db);

        String nodeQueryTemplate = "MATCH (n%s) RETURN id(n) AS id%s";
        String relsQueryTemplate = "MATCH (n)-[r%s]->(m) RETURN id(n) AS source, id(m) AS target%s";

        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            String labelString = maybeLabel.map(s -> ":" + s).orElse("");
            // CypherNodeLoader not yet supports parsing node props from return items ...
            String nodePropertiesString = nodeProperties.hasMappings()
                    ? ", " + nodeProperties.stream()
                    .map(PropertyMapping::neoPropertyKey)
                    .map(k -> "n." + k + " AS " + k)
                    .collect(Collectors.joining(", "))
                    : "";

            String nodeQuery = String.format(nodeQueryTemplate, labelString, nodePropertiesString);
            graphLoader.withLabel(nodeQuery);

            String relTypeString = maybeRelType.map(s -> ":" + s).orElse("");

            assert relProperties.numberOfMappings() <= 1;
            String relPropertiesString = relProperties.hasMappings()
                    ? ", " + relProperties.stream()
                    .map(PropertyMapping::propertyKey)
                    .map(k -> "r." + k + " AS weight")
                    .collect(Collectors.joining(", "))
                    : "";

            String relsQuery = String.format(relsQueryTemplate, relTypeString, relPropertiesString);
            graphLoader.withRelationshipType(relsQuery).withDeduplicationStrategy(DeduplicationStrategy.SKIP);
        } else {
            maybeLabel.ifPresent(graphLoader::withLabel);
            maybeRelType.ifPresent(graphLoader::withRelationshipType);
        }
        graphLoader.withRelationshipProperties(relProperties);
        graphLoader.withOptionalNodeProperties(nodeProperties);

        return graphLoader;
    }
}
