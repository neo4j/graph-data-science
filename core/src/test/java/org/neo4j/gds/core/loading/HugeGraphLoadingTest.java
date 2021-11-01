/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.core.loading;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class HugeGraphLoadingTest extends BaseTest {

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
            "org.neo4j.gds.core.utils.ArrayUtil.maxArrayLengthShift",
            String.valueOf(maxArrayLengthShift)
        );
        // something larger than one batch
        int offset = 10_000;
        int nodeCount = 60_000;

        var unused = Label.label("Unused");
        var label = Label.label("Foo");

        runInTransaction(db, tx -> {
            for (int i = 0; i < offset; i++) {
                tx.createNode(unused);
            }

            for (int j = offset; j < offset + nodeCount; j++) {
                Node node = tx.createNode(label);
                node.setProperty("bar", node.getId());
            }
        });

        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel(label.name())
            .addNodeProperty(PropertyMapping.of("bar", -1.0))
            .build()
            .graph();

        NodeProperties nodeProperties = graph.nodeProperties("bar");
        long propertyCountDiff = nodeCount - nodeProperties.size();
        String errorMessage = formatWithLocale(
            "Expected %d properties to be imported. Actually imported %d properties (missing %d properties).",
            nodeCount, nodeProperties.size(), propertyCountDiff
        );
        assertEquals(0, propertyCountDiff, errorMessage);

        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            double propertyValue = nodeProperties.doubleValue(nodeId);
            long neoId = graph.toOriginalNodeId(nodeId);
            assertEquals(
                neoId,
                (long) propertyValue,
                formatWithLocale("Property for node %d (neo = %d) was overwritten.", nodeId, neoId)
            );
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
                tx.createNode();
            }
        });

        final Graph graph = new StoreLoaderBuilder()
            .api(db)
            .build()
            .graph();

        assertEquals(nodeCount, graph.nodeCount());
    }

    @Test
    void testParallelEdgeWithHugeOffsetLoading() {
        org.neo4j.graphdb.RelationshipType fooRelType = org.neo4j.graphdb.RelationshipType.withName("FOO");
        int nodeCount = 1_000;
        int parallelEdgeCount = 10;

        runInTransaction(db, tx -> {
            Node n0 = tx.createNode();
            Node n1 = tx.createNode();
            Node last = null;

            for (int i = 0; i < nodeCount; i++) {
                last = tx.createNode();
            }

            n0.createRelationshipTo(n1, fooRelType).setProperty("weight", 1.0);
            for (int i = 0; i < parallelEdgeCount; i++) {
                n0.createRelationshipTo(last, fooRelType);
            }
        });

        final Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addRelationshipProperty(PropertyMapping.of("weight", 1.0))
            .build()
            .graph();

        assertEquals(11, graph.relationshipCount());
    }

    @Test
    void testMultipleRelationshipProjectionsOnTheSameType() {
        runQuery("CREATE" +
                 "  (a:Node {id: 0})" +
                 ", (b:Node {id: 1})" +
                 ", (a)-[:TYPE]->(b)");

        GraphStore graphStore = new StoreLoaderBuilder()
            .api(db)
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
            .graphStore();

        Graph natural = graphStore.getGraph(RelationshipType.of("TYPE_NATURAL"));
        assertGraphEquals(fromGdl("({id: 0})-->({id: 1})"), natural);

        Graph reverse = graphStore.getGraph(RelationshipType.of("TYPE_REVERSE"));
        assertGraphEquals(fromGdl("({id: 1})-->({id: 0})"), reverse);

        Graph undirected = graphStore.getGraph(RelationshipType.of("TYPE_UNDIRECTED"));
        assertGraphEquals(fromGdl("(a {id: 0})-->(b {id: 1}), (a)<--(b)"), undirected);

        Graph both = graphStore.getGraph(Arrays.asList(
            RelationshipType.of("TYPE_NATURAL"),
            RelationshipType.of("TYPE_REVERSE")
        ), Optional.empty());
        assertGraphEquals(fromGdl("(a {id: 0})-->(b {id: 1}), (a)<--(b)"), both);

        Graph union = graphStore.getUnion();
        assertGraphEquals(fromGdl("(a {id: 0})-->(b {id: 1.0}), (a)<--(b), (a)<--(b), (a)-->(b)"), union);
    }

    @Test
    void canIdentifyMultigraph() {
        runQuery("CREATE (a)-[:TYPE {t: 1}]->(b), (a)-[:TYPE {t: 2}]->(b), (a)-[:TYPE2]->(b)");

        GraphStore graphStore = new StoreLoaderBuilder()
            .api(db)
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_NONE",
                RelationshipProjection.of("TYPE", Aggregation.NONE)
            )
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_PROP_NONE",
                RelationshipProjection.builder()
                    .type("TYPE")
                    .properties(PropertyMappings.builder()
                        .addMapping(PropertyMapping.of("t", Aggregation.NONE))
                        .build())
                    .build()
            )
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_SINGLE",
                RelationshipProjection.of("TYPE", Aggregation.SINGLE)
            )
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_PROP_SINGLE",
                RelationshipProjection.builder()
                    .type("TYPE")
                    .properties(PropertyMappings.builder()
                        .addMapping(PropertyMapping.of("t", Aggregation.SINGLE))
                        .build())
                    .build()
            )
            .addRelationshipProjection(RelationshipProjection.of("TYPE2", Aggregation.SINGLE))
            .build()
            .graphStore();

        // using single rel type with aggregation guarantees no parallels
        assertFalse(graphStore.getGraph(RelationshipType.of("TYPE_SINGLE")).isMultiGraph());
        assertFalse(graphStore.getGraph(RelationshipType.of("TYPE_PROP_SINGLE")).isMultiGraph());

        // using a NONE may have a parallel (indeed has in this test)
        assertTrue(graphStore.getGraph(RelationshipType.of("TYPE_NONE")).isMultiGraph());
        assertTrue(graphStore.getGraph(RelationshipType.of("TYPE_PROP_NONE")).isMultiGraph());

        // using multiple rel types does not guarantee, even if they are all aggregated
        // if union graph improves, these conditions could change
        assertTrue(graphStore.getUnion().isMultiGraph());
        assertTrue(graphStore
            .getGraph(RelationshipType.of("TYPE_SINGLE"), RelationshipType.of("TYPE2"))
            .isMultiGraph());
        assertTrue(graphStore
            .getGraph(RelationshipType.of("TYPE_SINGLE"), RelationshipType.of("TYPE_PROP_SINGLE"))
            .isMultiGraph());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testMultiNodeLabelIndexScanWithExceedingBufferSizes(int concurrency) {
        int nodeCount = 150_000;

        var labelA = Label.label("A");
        var labelB = Label.label("B");
        var labelC = Label.label("C");

        var expectedLabelCounts = Map.of(
            labelA.name(), new MutableInt(),
            labelB.name(), new MutableInt(),
            labelC.name(), new MutableInt()
        );

        var rand = ThreadLocalRandom.current();

        runInTransaction(db, tx -> {
            for (int i = 0; i < nodeCount; i++) {
                var probability = rand.nextDouble();
                // Create node with all available labels
                if (probability < 1.0 / 6) {
                    expectedLabelCounts.values().forEach(MutableInt::increment);
                    tx.createNode(labelA, labelB, labelC);
                } else {
                    // Pick a single label based on probability
                    var label = (probability < 1.0 / 3) ? labelA : (probability < 2.0 / 3) ? labelB : labelC;
                    expectedLabelCounts.get(label.name()).increment();
                    tx.createNode(label);
                }
            }
        });

        var graphStore = new StoreLoaderBuilder()
            .addNodeLabel(labelA.name())
            .addNodeLabel(labelB.name())
            .addNodeLabel(labelC.name())
            .concurrency(concurrency)
            .api(db)
            .build()
            .graphStore();

        assertThat(graphStore.nodeCount()).isEqualTo(nodeCount);

        var actualLabelCounts = Map.of(
            labelA.name(), new MutableInt(),
            labelB.name(), new MutableInt(),
            labelC.name(), new MutableInt()
        );

        graphStore.nodes().forEachNode(node -> {
            graphStore.nodes().forEachNodeLabel(node, nodeLabel -> {
                actualLabelCounts.get(nodeLabel.name()).increment();
                return true;
            });
            return true;
        });

        var expectedTotalLabelCount = actualLabelCounts.values().stream().mapToInt(MutableInt::getValue).sum();
        var actualTotalLabelCount = actualLabelCounts.values().stream().mapToInt(MutableInt::getValue).sum();

        assertThat(actualTotalLabelCount).isEqualTo(expectedTotalLabelCount);
        assertThat(actualLabelCounts).isEqualTo(actualLabelCounts);
    }
}
