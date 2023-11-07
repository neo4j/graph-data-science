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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class NodeFilteredGraphIntersectTest {

    @GdlGraph(idOffset = 87)
    public static final String GRAPH =
        "(:A),(:A),(:A),(:A)," +
        "(a:B),(b:B),(c:B)," +
        "(a)-->(b),(b)-->(a)," +
        "(b)-->(c),(c)-->(b)" +
        "(c)-->(a),(a)-->(c)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @Test
    void testFilter() {
        var graph = graphStore.getGraph("B", RelationshipType.ALL_RELATIONSHIPS.name(), Optional.empty());
        var config = ImmutableRelationshipIntersectConfig.builder().build();
        var nodeCount = graph.nodeCount();

        assertThat(nodeCount).isEqualTo(3);
        assertThat(graph.relationshipCount()).isEqualTo(6);

        var intersect = new NodeFilteredGraphIntersect.NodeFilteredGraphIntersectFactory().load(graph, config);

        var triangleCount = new MutableInt(0);
        intersect.intersectAll(                 //triangles are found in reverse, so must change from 'a' to 'c'
            graph.toMappedNodeId(idFunction.of("c")),
            (nodeA, nodeB, nodeC) -> triangleCount.increment()
        );

        assertThat(triangleCount.intValue()).isEqualTo(1);
    }
}
