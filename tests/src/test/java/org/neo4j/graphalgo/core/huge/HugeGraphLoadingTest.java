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
package org.neo4j.graphalgo.core.huge;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;

public final class HugeGraphLoadingTest {

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void testDefaultPropertyLoading() {
        // default value
        testPropertyLoading(28);
    }

    @Test
    public void testPagedPropertyLoading() {
        // set low page shift so that 100k nodes will trigger the usage of the paged
        // huge array, which will trigger multi page code paths.
        // we import nodes in batches of 54600 nodes, using a page shift of 14
        // results in pages of 16384 elements, so we would have to write in multiple
        // pages for a single batch
        testPropertyLoading(14);
    }

    private void testPropertyLoading(int singlePageShift) {
        System.setProperty(
                "org.neo4j.graphalgo.core.utils.paged.HugeArrays.singlePageShift",
                String.valueOf(singlePageShift));
        // something larger than one batch
        int nodeCount = 60_000;
        Label label = Label.label("Foo");
        db.executeAndCommit(gdb -> {
            for (int j = 0; j < nodeCount; j++) {
                Node node = gdb.createNode(label);
                node.setProperty("bar", node.getId());
            }
        });

        final Graph graph = new GraphLoader(db)
                .withDirection(Direction.OUTGOING)
                .withLabel(label)
                .withOptionalNodeProperties(PropertyMapping.of("bar", "bar", -1.0))
                .load(HugeGraphFactory.class);

        HugeWeightMapping nodeProperties = graph.nodeProperties("bar");
        long propertyCountDiff = nodeCount - nodeProperties.size();
        String errorMessage = String.format(
                "Expected %d properties to be imported. Actually imported %d properties (missing %d properties).",
                nodeCount, nodeProperties.size(), propertyCountDiff
        );
        assertEquals(errorMessage, 0, propertyCountDiff);

        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            double weight = nodeProperties.nodeWeight(nodeId);
            long neoId = graph.toOriginalNodeId(nodeId);
            assertEquals(String.format("Property for node %d (neo = %d) was overwritten.", nodeId, neoId), neoId, (long) weight);
        }
    }

    @Test
    public void testFullPageLoading() {
        final int recordsPerPage = 546;

        // IdGeneration in Neo4j happens in chunks of 20. In order to get completely occupied pages
        // with 546 records each, we need to meet a point where we have a generated id chunk as well as a full page.
        // So we need a node count of 546 * 10 -> 5460 % 20 == 0
        final int pages = 10;
        int nodeCount = recordsPerPage * pages;

        db.executeAndCommit(gdb -> {
            for (int i = 0; i < nodeCount; i++) {
                gdb.createNode();
            }
        });

        final Graph graph = new GraphLoader(db).load(HugeGraphFactory.class);

        assertEquals(nodeCount, graph.nodeCount());
    }

    @Test
    public void testParallelEdgeWithHugeOffsetLoading() {
        RelationshipType fooRelType = RelationshipType.withName("FOO");
        int nodeCount = 1_000;
        int parallelEdgeCount = 10;

        try (Transaction tx = db.beginTx()) {
            Node n0 = db.createNode();
            Node n1 = db.createNode();
            Node last = null;

            for (int i = 0; i < nodeCount; i++) {
                last = db.createNode();
            }

            n0.createRelationshipTo(n1, fooRelType).setProperty("weight", 1.0);
            for (int i = 0; i < parallelEdgeCount; i++) {
                n0.createRelationshipTo(last, fooRelType);
            }
            tx.success();
        }

        final Graph graph = new GraphLoader(db)
                .withDirection(Direction.OUTGOING)
                .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                .load(HugeGraphFactory.class);

        Assert.assertEquals(2, graph.relationshipCount());
    }
}
