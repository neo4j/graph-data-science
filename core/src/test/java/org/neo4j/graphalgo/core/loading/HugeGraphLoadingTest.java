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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.compat.GraphDbApi;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.createNode;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.graphalgo.config.AlgoBaseConfig.ALL_NODE_LABELS;

final class HugeGraphLoadingTest {

    private GraphDbApi db;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @Test
    void testDefaultPropertyLoading() {
        // default value
        testPropertyLoading(28);
    }

    @Test
    void testPagedPropertyLoading() {
        // set low page shift so that 100k nodes will trigger the usage of the paged
        // huge array, which will trigger multi page code paths.
        // we import nodes in batches of 54600 nodes, using a page shift of 14
        // results in pages of 16384 elements, so we would have to write in multiple
        // pages for a single batch
        testPropertyLoading(14);
    }

    private void testPropertyLoading(int maxArrayLengthShift) {
        System.setProperty(
                "org.neo4j.graphalgo.core.utils.ArrayUtil.maxArrayLengthShift",
                String.valueOf(maxArrayLengthShift));
        // something larger than one batch
        int nodeCount = 60_000;
        Label label = Label.label("Foo");
        runInTransaction(db, tx -> {
            for (int j = 0; j < nodeCount; j++) {
                Node node = createNode(db, tx, label);
                node.setProperty("bar", node.getId());
            }
        });

        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel(label.name())
            .addNodeProperty(PropertyMapping.of("bar", -1.0))
            .loadAnyRelationshipType()
            .build()
            .graph(NativeFactory.class);

        NodeProperties nodeProperties = graph.nodeProperties("bar");
        long propertyCountDiff = nodeCount - nodeProperties.size();
        String errorMessage = String.format(
            "Expected %d properties to be imported. Actually imported %d properties (missing %d properties).",
            nodeCount, nodeProperties.size(), propertyCountDiff
        );
        assertEquals(0, propertyCountDiff, errorMessage);

        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            double propertyValue = nodeProperties.nodeProperty(nodeId);
            long neoId = graph.toOriginalNodeId(nodeId);
            assertEquals(neoId, (long) propertyValue, String.format("Property for node %d (neo = %d) was overwritten.", nodeId, neoId));
        }
    }

    @Test
    void testFullPageLoading() {
        final int recordsPerPage = 546;

        // IdGeneration in Neo4j happens in chunks of 20. In order to get completely occupied pages
        // with 546 records each, we need to meet a point where we have a generated id chunk as well as a full page.
        // So we need a node count of 546 * 10 -> 5460 % 20 == 0
        final int pages = 10;
        int nodeCount = recordsPerPage * pages;

        runInTransaction(db, tx -> {
            for (int i = 0; i < nodeCount; i++) {
                createNode(db, tx);
            }
        });

        final Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .build()
            .graph(NativeFactory.class);

        assertEquals(nodeCount, graph.nodeCount());
    }

    @Test
    void testParallelEdgeWithHugeOffsetLoading() {
        RelationshipType fooRelType = RelationshipType.withName("FOO");
        int nodeCount = 1_000;
        int parallelEdgeCount = 10;

        runInTransaction(db, tx -> {
            Node n0 = createNode(db, tx);
            Node n1 = createNode(db, tx);
            Node last = null;

            for (int i = 0; i < nodeCount; i++) {
                last = createNode(db, tx);
            }

            n0.createRelationshipTo(n1, fooRelType).setProperty("weight", 1.0);
            for (int i = 0; i < parallelEdgeCount; i++) {
                n0.createRelationshipTo(last, fooRelType);
            }
        });

        final Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .addRelationshipProperty(PropertyMapping.of("weight", 1.0))
            .build()
            .graph(NativeFactory.class);

        assertEquals(11, graph.relationshipCount());
    }

    @Test
    void testMultipleRelationshipProjectionsOnTheSameType() {
        runQuery(db, "CREATE" +
                     "  (a:Node {id: 0})" +
                     ", (b:Node {id: 1})" +
                     ", (a)-[:TYPE]->(b)");

        GraphStore graphStore = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_NATURAL",
                RelationshipProjection.of("TYPE", Orientation.NATURAL)
            )
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_REVERSE",
                RelationshipProjection.of("TYPE", Orientation.REVERSE)
            )
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_UNDIRECTED",
                RelationshipProjection.of("TYPE", Orientation.UNDIRECTED)
            )
            .addNodeProperty(PropertyMapping.of("id", 42.0))
            .build()
            .graphStore(NativeFactory.class);

        Graph natural = graphStore.getGraph("TYPE_NATURAL");
        assertGraphEquals(fromGdl("({id: 0})-->({id: 1})"), natural);

        Graph reverse = graphStore.getGraph("TYPE_REVERSE");
        assertGraphEquals(fromGdl("({id: 1})-->({id: 0})"), reverse);

        Graph undirected = graphStore.getGraph("TYPE_UNDIRECTED");
        assertGraphEquals(fromGdl("(a {id: 0})-->(b {id: 1}), (a)<--(b)"), undirected);

        Graph both = graphStore.getGraph(ALL_NODE_LABELS, Arrays.asList("TYPE_NATURAL", "TYPE_REVERSE"), Optional.empty());
        assertGraphEquals(fromGdl("(a {id: 0})-->(b {id: 1}), (a)<--(b)"), both);

        Graph union = graphStore.getUnion();
        assertGraphEquals(fromGdl("(a {id: 0})-->(b {id: 1}), (a)<--(b), (a)<--(b), (a)-->(b)"), union);
    }
}
