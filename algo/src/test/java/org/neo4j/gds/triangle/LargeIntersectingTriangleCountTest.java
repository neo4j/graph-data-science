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
package org.neo4j.gds.triangle;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class LargeIntersectingTriangleCountTest {

    private static final long TRIANGLE_COUNT = 4;

    ImmutableTriangleCountBaseConfig.Builder defaultConfigBuilder() {
      return ImmutableTriangleCountBaseConfig.builder();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testQueue(int concurrency) {

        long centerId = TRIANGLE_COUNT;

        var graph = produceRingStar(TRIANGLE_COUNT + 1l);

        var mappedCenterId = graph.toMappedNodeId(centerId);
        var result = IntersectingTriangleCount.create(
            graph,
            defaultConfigBuilder().concurrency(concurrency).build(),
            DefaultPool.INSTANCE
        ).compute();

        assertThat(result.globalTriangles()).isEqualTo(TRIANGLE_COUNT);

        var localTriangles = result.localTriangles();
        assertThat(localTriangles.get(mappedCenterId)).isEqualTo(TRIANGLE_COUNT);

        for (int u = 0; u < localTriangles.size(); u++) {
            if (u == mappedCenterId) {
                continue;
            }
            assertThat(localTriangles.get(u)).isEqualTo(2L);
        }

    }

    private Graph produceRingStar(long nodeCount) {
        long centerOriginalId = nodeCount - 1;

        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(nodeCount)
            .concurrency(1)
            .build();

        for (var nodeId = 0; nodeId < nodeCount; ++nodeId) {
            nodesBuilder.addNode(nodeId);
        }

        var idMap = nodesBuilder.build().idMap();

        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .relationshipType(RelationshipType.of("FOO"))
            .orientation(Orientation.UNDIRECTED)
            .executorService(DefaultPool.INSTANCE)
            .build();

        for (long u = 0; u < nodeCount - 1; ++u) {
            long v = (u + 1) % (nodeCount - 1);
            relationshipsBuilder.add(u, v);
        }

        for (long u = 0; u < nodeCount - 1; ++u) {
            relationshipsBuilder.add(u, centerOriginalId);
        }

        var relationships = relationshipsBuilder.build();

        return GraphFactory.create(idMap, relationships);
    }
}
