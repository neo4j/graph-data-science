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
package org.neo4j.gds.triangle.intersect;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.huge.UnionGraph;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.triangle.LabelFilterChecker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

final class CompositeIntersectionTest {

    private static final int DEGREE = 25;


    @Test
    void intersectWithTargets() {

        ArrayList<Long> targets = new ArrayList<>();
        var graph = produceGraph(targets);
        var targetIterator = targets.iterator();

        var start1 = Math.min(graph.toMappedNodeId(DEGREE + 1), graph.toMappedNodeId(DEGREE));
        var start2 = Math.max(graph.toMappedNodeId(DEGREE + 1), graph.toMappedNodeId(DEGREE));

        var intersect = new UnionGraphIntersect.UnionGraphIntersectFactory().load(
            graph,
            Long.MAX_VALUE,
            new LabelFilterChecker(Collections.emptyList(), graph::hasLabel)
        );

        intersect.intersectAll(
            start2, (a, b, c) -> {
                Long next = targetIterator.next();
                var targetMappedId = graph.toMappedNodeId(next);
                assertThat(a).isEqualTo(targetMappedId);
                assertThat(b).isEqualTo(start1);
                assertThat(c).isEqualTo(start2);
            }
        );

        assertThat(targetIterator.hasNext()).isFalse();

    }

    @Test
    void intersectWithTargetsWithMaxDegree() {
        var targets = new ArrayList<Long>();
        var graph = produceGraph(targets);

        var start2 = Math.max(graph.toMappedNodeId(DEGREE + 1), graph.toMappedNodeId(DEGREE));

        var intersect = new UnionGraphIntersect.UnionGraphIntersectFactory().load(
            graph,
            0,
            new LabelFilterChecker(Collections.emptyList(), graph::hasLabel)
        );
        assertThatNoException().isThrownBy(
            () ->
                intersect.intersectAll(
                    start2, (a, b, c) ->
                    {
                        throw new IllegalArgumentException();
                    }
                )
        );

    }

    Graph produceGraph(ArrayList<Long> targets) {
        Random random = new Random(0);
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(2 + DEGREE)
            .concurrency(new Concurrency(1))
            .build();

        for (long i = 0; i < 3 + DEGREE; ++i) {
            nodesBuilder.addNode(i);
        }

        var idMap = nodesBuilder.build().idMap();
        RelationshipsBuilder relationshipsBuilder1 = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .relationshipType(org.neo4j.gds.RelationshipType.of("FOO"))
            .orientation(Orientation.UNDIRECTED)
            .executorService(DefaultPool.INSTANCE)
            .build();

        RelationshipsBuilder relationshipsBuilder2 = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .relationshipType(org.neo4j.gds.RelationshipType.of("BAR"))
            .orientation(Orientation.UNDIRECTED)
            .executorService(DefaultPool.INSTANCE)
            .build();


        long start0 = DEGREE + 2, start1 = DEGREE + 1, start2 = DEGREE;
        relationshipsBuilder1.add(start1, start2);

        for (int targetId = 0; targetId < start2; targetId++) {
            var relationshipBuilder = relationshipsBuilder1;
            if (random.nextBoolean()) {
                relationshipBuilder = relationshipsBuilder2;
            }
            relationshipBuilder.add(start1, targetId);
            relationshipBuilder.add(start0, targetId);

            if (random.nextBoolean()) {
                relationshipBuilder.add(start2, targetId);
                targets.add((long) targetId);
            }
        }

        var relationships1 = relationshipsBuilder1.build();
        var relationships2 = relationshipsBuilder2.build();

        var graph1 = GraphFactory.create(idMap, relationships1);
        var graph2 = GraphFactory.create(idMap, relationships2);

        return UnionGraph.of(List.of(graph1, graph2));

    }
    @GdlExtension
    @Nested
    class UnionGraphWithFilters {
        @GdlGraph(idOffset = 87, orientation = Orientation.UNDIRECTED)
        public static final String GRAPH =
            "(:A),(:A),(:A),(:A)," +
                "(a:B),(b:B),(c:B)," +
                "(a)-[:R1]->(b)," +
                "(b)-[:R]->(c)," +
                "(c)-[:R]->(a)";
        @Inject
        private GraphStore graphStore;

        @Inject
        private IdFunction idFunction;

        @Test
        void testFilter() {
            var graph = graphStore.getGraph(
                List.of(NodeLabel.of("B")),
                List.of(RelationshipType.of("R"), RelationshipType.of("R1")),
                Optional.empty()
            );
            var maxDegree = Long.MAX_VALUE;
            var nodeCount = graph.nodeCount();

                assertThat(nodeCount).isEqualTo(3);
                assertThat(graph.relationshipCount()).isEqualTo(6);

            var intersect = new UnionGraphIntersect.NodeFilteredUnionGraphIntersectFactory().load(
                graph,
                maxDegree,
                new LabelFilterChecker(Collections.emptyList(), graph::hasLabel)
            );

            var triangleCount = new MutableInt(0);
            intersect.intersectAll(                 //triangles are found in reverse, so must change from 'a' to 'c'
                graph.toMappedNodeId(idFunction.of("c")),
                (nodeA, nodeB, nodeC) -> triangleCount.increment()
            );

            assertThat(triangleCount.intValue()).isEqualTo(1);
        }
    }
}
