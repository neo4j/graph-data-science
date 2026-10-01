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
package org.neo4j.gds.mem;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

class GraphStoreMemoryUsageFactoryTest {

    @Test
    void shouldReturnEmptyMapWhenNoRelationshipTypes() {
        var graphStore  = GdlFactory.builder().gdlGraph("(a),(b)").build().build();
        assertThatNoException().isThrownBy(() -> GraphStoreMemoryUsageFactory.of(graphStore));

    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldAccountForAllPagesOfALargeGraph() {
        var nodeCount = 1_000_000L;
        var graphStore = RandomGraphGenerator.builder()
            .nodeCount(nodeCount)
            .averageDegree(5)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(42)
            .build()
            .generateGraphstore(DatabaseId.EMPTY, new Concurrency(4));
        var relationshipCount = graphStore.relationshipCount();

        var memoryUsage = GraphStoreMemoryUsageFactory.of(graphStore);
        var details = memoryUsage.detailSizeInBytes();
        var nodes = (Map<String, Long>) details.get("nodes");
        var relationships = (Map<String, Long>) details.get("relationships");
        var adjacencyList = ((Map<String, Map<String, Object>>) details.get("adjacencyLists")).get("REL");

        // the adjacency list must span multiple pages for this test to be meaningful
        assertThat((long) adjacencyList.get("pages")).isGreaterThan(1L);

        // bounds derived from the graph dimensions, not from the walker
        var minOffsets = Estimate.sizeOfLongArray(nodeCount);
        var minDegrees = Estimate.sizeOfIntArray(nodeCount);
        var minForwardMapping = Estimate.sizeOfLongArray(nodeCount);
        var minBackwardMapping = nodeCount * Long.BYTES;
        var minTargetIds = relationshipCount; // at least one byte per target id

        // single arrays only add their wrapper object
        var maxOffsets = minOffsets + KIB;
        var maxDegrees = minDegrees + KIB;
        var maxForwardMapping = minForwardMapping + KIB;
        // sparse pages add headers and a partially filled last page
        var maxBackwardMapping = minBackwardMapping + minBackwardMapping / 20;
        // compressed target ids must not exceed their uncompressed size
        var maxTargetIds = relationshipCount * Long.BYTES;

        assertThat(relationships.get("offsets")).isBetween(minOffsets, maxOffsets);
        assertThat(relationships.get("degrees")).isBetween(minDegrees, maxDegrees);
        assertThat(relationships.get("targetIds")).isBetween(minTargetIds, maxTargetIds);
        assertThat(nodes.get("forwardMapping")).isBetween(minForwardMapping, maxForwardMapping);
        assertThat(nodes.get("backwardMapping")).isBetween(minBackwardMapping, maxBackwardMapping);

        var minTotal = minOffsets + minDegrees + minTargetIds + minForwardMapping + minBackwardMapping;
        // allow for schema, labels and other small objects
        var maxTotal = maxOffsets + maxDegrees + maxTargetIds + maxForwardMapping + maxBackwardMapping + 256 * KIB;
        assertThat(memoryUsage.sizeInBytes()).isBetween(minTotal, maxTotal);
        // used by gds.graph.list
        assertThat(MemoryUsage.sizeOf(graphStore)).isBetween(minTotal, maxTotal);
    }

    private static final long KIB = 1024L;
}
