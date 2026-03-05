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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.construction.NodesBuilderBuilder;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.HashSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

class HugeSimilarityGraphTest {

    @Test
    void shouldReturnΑll() {
        var nodesBuilder = new NodesBuilderBuilder().maxOriginalId(3).build();
        nodesBuilder.addNode(0);
        nodesBuilder.addNode(1);
        nodesBuilder.addNode(2);
        nodesBuilder.addNode(3);
        var idMap = nodesBuilder.build().idMap();
        var similarityGraph = SimilarityGraphNewBuilder.build(
            true,
            Stream.of(
                new SimilarityResult(0, 1, 10),
                new SimilarityResult(1, 2, 5),
                new SimilarityResult(1, 3, 9)
            ),
            idMap,
            new Concurrency(1),
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );
        var histogram = similarityGraph.similarityDistribution();
        assertThat(histogram.get("mean")).asInstanceOf(DOUBLE).isCloseTo(8.0, Offset.offset(1e-3));

        assertThat(similarityGraph.nodeCount()).isEqualTo(4L);
        assertThat(similarityGraph.relationshipCount()).isEqualTo(3L);
        HashSet<SimilarityResult> similarityResults = new HashSet<>();
        similarityGraph.forEachRelationship(1, 1.0, (s, t, w) -> similarityResults.add(new SimilarityResult(s, t, w)));
        assertThat(similarityResults).containsExactlyInAnyOrder(
            new SimilarityResult(1, 2, 5),
            new SimilarityResult(1, 3, 9)
        );
        var rels = similarityGraph.relationships("foo", "bar");
        AdjacencyCursor adjacencyCursor = rels.topology().adjacencyList().adjacencyCursor(1);
        assertThat(adjacencyCursor.nextVLong()).isEqualTo(2L);
        assertThat(adjacencyCursor.nextVLong()).isEqualTo(3L);

    }

        @Test
    void shouldReturnEmptyDistributionIfUnset() {
        var nodesBuilder = new NodesBuilderBuilder().maxOriginalId(3).build();
        nodesBuilder.addNode(0);
        nodesBuilder.addNode(1);
        nodesBuilder.addNode(2);
        nodesBuilder.addNode(3);
        var idMap = nodesBuilder.build().idMap();
        var stats = SimilarityGraphNewBuilder.build(
            false,
            Stream.of(
                new SimilarityResult(0, 1, 10),
                new SimilarityResult(1, 2, 5),
                new SimilarityResult(1, 3, 9)
            ),
            idMap,
            new Concurrency(1),
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );
        var histogram = stats.similarityDistribution();
        assertThat(histogram).isEmpty();
    }

}
