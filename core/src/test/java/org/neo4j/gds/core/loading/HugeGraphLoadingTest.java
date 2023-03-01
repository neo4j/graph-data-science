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

import com.carrotsearch.hppc.LongDoubleHashMap;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
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
            "org.neo4j.gds.collections.ArrayUtil.maxArrayLengthShift",
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
            .databaseService(db)
            .addNodeLabel(label.name())
            .addNodeProperty(PropertyMapping.of("bar", -1.0))
            .build()
            .graph();

        NodePropertyValues nodePropertyValues = graph.nodeProperties("bar");
        long propertyCountDiff = nodeCount - nodePropertyValues.valuesStored();
        String errorMessage = formatWithLocale(
            "Expected %d properties to be imported. Actually imported %d properties (missing %d properties).",
            nodeCount, nodePropertyValues.valuesStored(), propertyCountDiff
        );
        assertEquals(0, propertyCountDiff, errorMessage);

        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            double propertyValue = nodePropertyValues.doubleValue(nodeId);
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
            .databaseService(db)
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
            .databaseService(db)
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
            .databaseService(db)
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
        assertGraphEquals(fromGdl("({id: 0})-[:TYPE_NATURAL]->({id: 1})"), natural);

        Graph reverse = graphStore.getGraph(RelationshipType.of("TYPE_REVERSE"));
        assertGraphEquals(fromGdl("({id: 1})-[:TYPE_REVERSE]->({id: 0})"), reverse);

        Graph undirected = graphStore.getGraph(RelationshipType.of("TYPE_UNDIRECTED"));
        assertGraphEquals(fromGdl("(a {id: 0})-[:TYPE_UNDIRECTED]->(b {id: 1}), (a)<-[:TYPE_UNDIRECTED]-(b)"), undirected);

        Graph both = graphStore.getGraph(Arrays.asList(
            RelationshipType.of("TYPE_NATURAL"),
            RelationshipType.of("TYPE_REVERSE")
        ), Optional.empty());
        assertGraphEquals(fromGdl("(a {id: 0})-[:TYPE_NATURAL]->(b {id: 1}), (a)<-[:TYPE_REVERSE]-(b)"), both);

        Graph union = graphStore.getUnion();
        assertGraphEquals(fromGdl("(a {id: 0})-[:TYPE_NATURAL]->(b {id: 1.0}), (a)<-[:TYPE_REVERSE]-(b), (a)<-[:TYPE_UNDIRECTED]-(b), (a)-[:TYPE_UNDIRECTED]->(b)"), union);
    }

    @Test
    void canIdentifyMultigraph() {
        runQuery("CREATE (a)-[:TYPE {t: 1}]->(b), (a)-[:TYPE {t: 2}]->(b), (a)-[:TYPE2]->(b)");

        GraphStore graphStore = new StoreLoaderBuilder()
            .databaseService(db)
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

        var labelABits = new Roaring64Bitmap();
        var labelBBits = new Roaring64Bitmap();

        runInTransaction(db, tx -> {
            for (int i = 0; i < nodeCount; i++) {
                if (i % 3 == 0) {
                    labelABits.add(tx.createNode(labelA).getId());
                } else if (i % 3 == 1) {
                    labelBBits.add(tx.createNode(labelB).getId());
                } else {
                    var id = tx.createNode(labelA, labelB).getId();
                    labelABits.add(id);
                    labelBBits.add(id);
                }
            }
        });

        var graphStore = new StoreLoaderBuilder()
            .addNodeLabel(labelA.name())
            .addNodeLabel(labelB.name())
            .concurrency(concurrency)
            .databaseService(db)
            .build()
            .graphStore();

        assertThat(graphStore.nodeCount()).isEqualTo(nodeCount);

        graphStore.nodes().forEachNode(node -> {
            var nodeLabels = graphStore
                .nodes()
                .nodeLabels(node)
                .stream()
                .map(NodeLabel::name)
                .collect(Collectors.toSet());

            var neoId = graphStore.nodes().toOriginalNodeId(node);

            if (labelABits.contains(neoId) && !labelBBits.contains(neoId)) {
                assertThat(nodeLabels).contains(labelA.name());
                assertThat(nodeLabels).doesNotContain(labelB.name());
            }
            else if (!labelABits.contains(neoId) && labelBBits.contains(neoId)) {
                assertThat(nodeLabels).doesNotContain(labelA.name());
                assertThat(nodeLabels).contains(labelB.name());
            } else if (labelABits.contains(neoId) && labelBBits.contains(neoId)) {
                assertThat(nodeLabels).contains(labelA.name());
                assertThat(nodeLabels).contains(labelB.name());
            } else {
                Assertions.fail("no labels found for id: " + node);
            }

            return true;
        });
    }

    @Test
    void testMultipleRelationshipPropertiesAcrossMultiplePages() {
        var fooRelType = org.neo4j.graphdb.RelationshipType.withName("FOO");
        int nodeCount = BumpAllocator.PAGE_SIZE * 2;
        var centerNeoNodeId = new MutableLong();
        var expectedProperties = new LongDoubleHashMap();

        runInTransaction(db, tx -> {
            var center = tx.createNode();
            centerNeoNodeId.setValue(center.getId());

            for (int i = 0; i < nodeCount; i++) {
                var node = tx.createNode();
                var edge = node.createRelationshipTo(center, fooRelType);
                double property = 42.0 + i;
                edge.setProperty("weight", property);
                expectedProperties.put(node.getId(), property);
            }
        });

        var graph = new StoreLoaderBuilder()
            .databaseService(db)
            .addRelationshipProperty(PropertyMapping.of("p1", "weight", 1.0))
            .addRelationshipProperty(PropertyMapping.of("p2", "weight", 1.0))
            .concurrency(4)
            .build()
            .graph();

        long centerNodeId = graph.toMappedNodeId(centerNeoNodeId.longValue());

        for (var expectedCursor : expectedProperties) {
            long source = graph.toMappedNodeId(expectedCursor.key);
            graph.forEachRelationship(source, 2.0, (s, t, property) -> {
                assertThat(t).isEqualTo(centerNodeId);
                assertThat(property).isEqualTo(expectedCursor.value, within(1e-5));
                return true;
            });
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testSingleLabelPartitionedTokenIndexRespectingBatchSize(int concurrency) {
        // batchSize = prefetch size * PAGE_SIZE / NodeRecord size
        var expectedBatchSize = 54_656;

        var label = Label.label("Node");
        long labelCount = 2 * expectedBatchSize;

        runInTransaction(db, tx -> {
            for (int i = 0; i < labelCount; i++) {
                tx.createNode(label);
            }
        });

        GdsFeatureToggles.USE_PARTITIONED_SCAN.enableAndRun(() -> {
            var graph = new StoreLoaderBuilder()
                .databaseService(db)
                .addNodeLabel(label.name())
                .concurrency(concurrency)
                .build()
                .graph();

            assertThat(graph.nodeCount()).isEqualTo(labelCount);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testMultiLabelPartitionedTokenIndexRespectingBatchSize(int concurrency) {
        // batchSize = prefetch size * PAGE_SIZE / NodeRecord size
        var expectedBatchSize = 54_656;

        var labelA = Label.label("A");
        var labelB = Label.label("B");
        long labelACount = 2 * expectedBatchSize;
        long labelBCount = 3 * expectedBatchSize;
        long labelABCount = 1 * expectedBatchSize;

        runInTransaction(db, tx -> {
            for (int i = 0; i < labelACount; i++) {
                tx.createNode(labelA);
            }
            for (int i = 0; i < labelBCount; i++) {
                tx.createNode(labelB);
            }
            for (int i = 0; i < labelABCount; i++) {
                tx.createNode(labelA, labelB);
            }
        });

        GdsFeatureToggles.USE_PARTITIONED_SCAN.enableAndRun(() -> {
            var graph = new StoreLoaderBuilder()
                .databaseService(db)
                .addNodeLabel(labelA.name())
                .addNodeLabel(labelB.name())
                .concurrency(concurrency)
                .build()
                .graph();

            assertThat(graph.nodeCount()).isEqualTo(labelACount + labelBCount + labelABCount);
        });
    }
}
