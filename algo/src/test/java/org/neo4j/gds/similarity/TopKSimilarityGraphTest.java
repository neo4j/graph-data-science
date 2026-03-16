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
package org.neo4j.gds.similarity;

import com.carrotsearch.hppc.BitSet;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.similarity.nodesim.TopKGraph;
import org.neo4j.gds.similarity.nodesim.TopKMap;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.GdlTestSupport.fromGdl;

class TopKSimilarityGraphTest {

    @Test
    void shouldReturnDistribution() {
        var bitset = new BitSet(4);
        bitset.set(0, 2);
        var topKMap = new TopKMap(4, bitset, 2, true);

        topKMap.put(0, 1, 10);
        topKMap.put(1, 2, 5);
        topKMap.put(1, 3, 9);

        var graph = fromGdl("CREATE (a),(b),(c),(d)"); //we need that for the for-each
        var topGraph = new TopKGraph(graph, topKMap);
        var similarityGraph = new TopKSimilarityGraph(
            topGraph,
            true
        );

        assertThat(similarityGraph.shouldComputeDistribution()).isTrue();
        var histogram = similarityGraph.similarityDistribution();
        assertThat(histogram.get("mean")).asInstanceOf(DOUBLE).isCloseTo(8.0, Offset.offset(1e-3));
        assertThat(similarityGraph.shouldComputeDistribution()).isFalse();

    }

    @Test
    void shouldComputeDistributionWhileCreatingRelationships() {
        var bitset = new BitSet(4);
        bitset.set(0, 2);
        var topKMap = new TopKMap(4, bitset, 2, true);

        topKMap.put(0, 1, 10);
        topKMap.put(1, 2, 5);
        topKMap.put(1, 3, 9);

        var graph = fromGdl("CREATE (a),(b),(c),(d)"); //we need that for the for-each
        var topGraph = new TopKGraph(graph, topKMap);
        var similarityGraph = new TopKSimilarityGraph(
            topGraph,
            true
        );

        assertThat(similarityGraph.shouldComputeDistribution()).isTrue();
        var rels = similarityGraph.relationships("foo", "bar");
        assertThat(similarityGraph.shouldComputeDistribution()).isFalse();
        var histogram = similarityGraph.similarityDistribution();
        assertThat(histogram.get("mean")).asInstanceOf(DOUBLE).isCloseTo(8.0, Offset.offset(1e-3));
        var similarityResults  = new HashSet<SimilarityResult>();
        similarityGraph.forEachRelationship(1, 1.0, (s, t, w) -> similarityResults.add(new SimilarityResult(s, t, w)));
        assertThat(similarityResults).containsExactlyInAnyOrder(
            new SimilarityResult(1, 2, 5),
            new SimilarityResult(1, 3, 9)
        );
        AdjacencyCursor adjacencyCursor = rels.topology().adjacencyList().adjacencyCursor(1);
        assertThat(adjacencyCursor.nextVLong()).isEqualTo(2L);
        assertThat(adjacencyCursor.nextVLong()).isEqualTo(3L);


    }

    @Test
    void shouldReturnEmptyDistributionIfUnset() {
        var bitset = new BitSet(4);
        bitset.set(0, 2);
        var topKMap = new TopKMap(4, bitset, 2, true);
        topKMap.put(0, 1, 10);
        topKMap.put(1, 2, 5);
        topKMap.put(1, 3, 9);

        var graph = mock(Graph.class);
        when(graph.nodeCount()).thenReturn(4L);
        var topGraph = new TopKGraph(graph, topKMap);
        var similarityGraph = new TopKSimilarityGraph(
            topGraph,
            false
        );

        assertThat(similarityGraph.shouldComputeDistribution()).isFalse();

    }
}
