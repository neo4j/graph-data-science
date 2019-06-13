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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class GraphNegativeTest extends RandomGraphTestCase {

    private Class<? extends GraphFactory> graphImpl;

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    @Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"}
        );
    }

    @SuppressWarnings("unchecked")
    public GraphNegativeTest(
            Class<?> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = (Class<? extends GraphFactory>) graphImpl;
    }

    @Test
    public void shouldLoadAllNodesForNonExistingStringLabel() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withLabel("foo")
                .load(graphImpl);
        assertEquals(graph.nodeCount(), 0);
    }

    @Test
    public void shouldLoadAllNodesForNonExistingLabel() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withLabel(Label.label("foo"))
                .load(graphImpl);
        assertEquals(graph.nodeCount(), 0);
    }

    @Test
    public void shouldLoadRelationshipsNodesForNonExistingStringTypes() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType("foo")
                .load(graphImpl);
        assertEquals(graph.relationshipCount(), 0);
    }

    @Test
    public void shouldLoadRelationshipsNodesForNonExistingTypes() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType(RelationshipType.withName("foo"))
                .load(graphImpl);
        assertEquals(graph.relationshipCount(), 0);
    }

    @Test
    @Ignore("Ignoring until Paul has implemented deduplication logic")
    public void shouldLoadWeightedRelationshipsNodesForNonExistingStringTypes() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType("foo")
                .withRelationshipWeightsFromProperty("weight", 42.0)
                .load(graphImpl);
        testWeightedRelationships(graph);
    }

    @Test
    @Ignore("Ignoring until Paul has implemented deduplication logic")
    public void shouldLoadWeightedRelationshipsNodesForNonExistingTypes() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType(RelationshipType.withName("foo"))
                .withRelationshipWeightsFromProperty("weight", 42.0)
                .load(graphImpl);
        testWeightedRelationships(graph);
    }

    @Test
    public void shouldLoadDefaultWeightForNonExistingProperty() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipWeightsFromProperty("foo", 13.37)
                .load(graphImpl);
        graph.forEachNode(node -> {
            graph.forEachRelationship(
                    node,
                    Direction.OUTGOING,
                    (start, end, weight) -> {
                        assertEquals(13.37, weight, 0.0001);
                        return true;
                    });
            return true;
        });
    }

    private void testRelationships(final Graph graph) {
        testAnyRelationship(graph, (w, r) -> {});
    }

    private void testWeightedRelationships(final Graph graph) {
        testAnyRelationship(graph, (w, rel) -> collector.checkThat(
                "wrong weight for " + failMsg(rel),
                w,
                closeTo(((Number) rel.getProperty("weight")).doubleValue(), 0.001)
        ));
    }

    private <T extends RelationshipCursor> void testAnyRelationship(
            Graph graph,
            BiConsumer<Double, Relationship> tester) {
        try (Transaction tx = db.beginTx()) {
            ResourceIterable<Relationship> rels = db.getAllRelationships();
            ResourceIterator<Relationship> iterator = rels.iterator();
            while (iterator.hasNext()) {
                Relationship rel = iterator.next();
                final boolean[] hasRelation = {false};
                long startNode = rel.getStartNode().getId();
                long startId = graph.toMappedNodeId(startNode);
                long endId = graph.toMappedNodeId(rel.getEndNodeId());
                graph.forEachRelationship(startId, Direction.OUTGOING, (src, tgt) -> {
                    if (src == startId && tgt == endId) {
                        hasRelation[0] = true;
                        collector.checkThat(
                                failMsg(rel),
                                rel.getEndNode().getId(),
                                is(graph.toOriginalNodeId(tgt))
                        );
                    }
                    return true;
                });
                // test weighted consumer as well
                graph.forEachRelationship(startId, Direction.OUTGOING, (src, tgt, weight) -> {
                    if (src == startId && tgt == endId) {
                        collector.checkThat(
                                failMsg(rel),
                                rel.getEndNode().getId(),
                                is(graph.toOriginalNodeId(tgt)));
                        tester.accept(weight, rel);
                    }
                    return true;
                });
                collector.checkThat(
                        "did not find relation " + failMsg(rel),
                        hasRelation[0],
                        is(true)
                );
            }
            iterator.close();
            tx.success();
        }
    }

    private static String failMsg(Relationship rel) {
        return String.format(
                "(%s)-[%s]->(%s)",
                rel.getStartNode().getId(),
                rel.getId(),
                rel.getEndNode().getId());
    }
}
