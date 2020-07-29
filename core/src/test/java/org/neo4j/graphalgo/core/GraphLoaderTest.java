/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestGraphLoader;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.TestSupport.AllGraphStoreFactoryTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.TerminationFlag;

import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.assertTransactionTermination;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class GraphLoaderTest extends BaseTest {

    public static final String DB_CYPHER =
        "CREATE" +
        "  (n1:Node1 {prop1: 1})" +
        ", (n2:Node2 {prop2: 2})" +
        ", (n3:Node3 {prop3: 3})" +
        ", (n1)-[:REL1 {prop1: 1}]->(n2)" +
        ", (n1)-[:REL2 {prop2: 2}]->(n3)" +
        ", (n2)-[:REL1 {prop3: 3, weight: 42}]->(n3)" +
        ", (n2)-[:REL3 {prop4: 4, weight: 1337}]->(n3)";

    @BeforeEach
    void setup() {
        runQuery(DB_CYPHER);
    }

    @AllGraphStoreFactoryTypesTest
    void testAnyLabel(TestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoader.from(db).withDefaultAggregation(Aggregation.SINGLE).graph(factoryType);
        assertGraphEquals(fromGdl("(a)-->(b), (a)-->(c), (b)-->(c)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithLabel(TestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoader.from(db).withLabels("Node1").graph(factoryType);
        assertGraphEquals(fromGdl("(:Node1)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithMultipleLabels(TestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoader.from(db).withLabels("Node1", "Node2").graph(factoryType);
        assertGraphEquals(fromGdl("(a:Node1)-->(b:Node2)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithMultipleLabelsAndProperties(TestSupport.FactoryType factoryType) {
        PropertyMappings properties = PropertyMappings.of(PropertyMapping.of("prop1", 42.0));
        PropertyMappings multipleProperties = PropertyMappings.of(
            PropertyMapping.of("prop1", 42.0),
            PropertyMapping.of("prop2", 42.0)
        );

        Graph graph = TestGraphLoader.from(db)
            .withLabels("Node1", "Node2")
            .withNodeProperties(properties)
            .graph(factoryType);
        assertGraphEquals(fromGdl("(a:Node1 {prop1: 1.0})-->(b:Node2 {prop1: 42.0})"), graph);

        graph = TestGraphLoader.from(db)
            .withLabels("Node1", "Node2")
            .withNodeProperties(multipleProperties)
            .graph(factoryType);
        assertGraphEquals(fromGdl("(a:Node1 {prop1: 1.0, prop2: 42.0})-->(b:Node2 {prop1: 42.0, prop2: 2.0})"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithSingleLabelAndProperties(TestSupport.FactoryType factoryType) {
        PropertyMappings properties = PropertyMappings.of(PropertyMapping.of("prop1", 42.0));
        PropertyMappings multipleProperties = PropertyMappings.of(
            PropertyMapping.of("prop1", 42.0),
            PropertyMapping.of("prop2", 42.0)
        );

        Graph graph = TestGraphLoader.from(db)
            .withLabels("Node1")
            .withNodeProperties(properties)
            .graph(factoryType);
        assertGraphEquals(fromGdl("(a:Node1 {prop1: 1.0})"), graph);

        graph = TestGraphLoader.from(db)
            .withLabels("Node1")
            .withNodeProperties(multipleProperties)
            .graph(factoryType);
        assertGraphEquals(fromGdl("(a:Node1 {prop1: 1.0, prop2: 42.0})"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testAnyRelation(TestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoader.from(db).withDefaultAggregation(Aggregation.SINGLE).graph(factoryType);
        assertGraphEquals(fromGdl("(a)-->(b), (a)-->(c), (b)-->(c)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithBothWeightedRelationship(TestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoader.from(db)
            .withRelationshipTypes("REL3")
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .graph(factoryType);

        assertGraphEquals(fromGdl("(), ()-[{w:1337}]->()"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithOutgoingRelationship(TestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoader.from(db)
            .withRelationshipTypes("REL3")
            .graph(factoryType);
        assertGraphEquals(fromGdl("(), ()-->()"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithNodeProperties(TestSupport.FactoryType factoryType) {
        PropertyMappings nodePropertyMappings = PropertyMappings.of(
            PropertyMapping.of("prop1", "prop1", 0D),
            PropertyMapping.of("prop2", "prop2", 0D),
            PropertyMapping.of("prop3", "prop3", 0D)
        );

        Graph graph = TestGraphLoader
            .from(db)
            .withNodeProperties(nodePropertyMappings)
            .withDefaultAggregation(Aggregation.SINGLE)
            .graph(factoryType);

        Graph expected = fromGdl("(a {prop1: 1.0, prop2: 0.0, prop3: 0.0})" +
                               "(b {prop1: 0.0, prop2: 2.0, prop3: 0.0})" +
                               "(c {prop1: 0.0, prop2: 0.0, prop3: 3.0})" +
                               "(a)-->(b), (a)-->(c), (b)-->(c)");
        assertGraphEquals(expected, graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithRelationshipProperty(TestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoader.from(db)
            .withRelationshipProperties(PropertyMapping.of("weight","prop1", 3.14))
            .withDefaultAggregation(Aggregation.SINGLE)
            .graph(factoryType);
        assertGraphEquals(fromGdl("(a)-[{w: 1}]->(b), (a)-[{w: 3.14D}]->(c), (b)-[{w: 3.14D}]->(c)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testLoadCorrectLabelCombinations(TestSupport.FactoryType factoryType) {
        runQuery("CREATE (n:Node1:Node2)");
        Graph graph = TestGraphLoader.from(db)
            .withLabels("Node1", "Node2")
            .graph(factoryType);
        assertGraphEquals(fromGdl("(a:Node1), (b:Node2), (c:Node1:Node2), (a)-->(b)"), graph);
    }

    @Test
    void testLoadNodeWithMultipleLabelsOnPartialLabelMatch() {
        runQuery("CREATE (n:Node1:Node2)");
        Graph graph = TestGraphLoader.from(db)
            .withLabels("Node1")
            .graph(TestSupport.FactoryType.NATIVE);
        assertGraphEquals(fromGdl("(a:Node1), (c:Node1)"), graph);
    }

    @Test
    void stopsImportingWhenTransactionHasBeenTerminated() {
        TerminationFlag terminationFlag = () -> false;
        assertTransactionTermination(
            () -> new StoreLoaderBuilder()
                .api(db)
                .terminationFlag(terminationFlag)
                .build()
                .graph()
        );
    }
}
