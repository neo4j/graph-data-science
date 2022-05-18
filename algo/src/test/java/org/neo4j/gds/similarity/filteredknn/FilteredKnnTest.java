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
package org.neo4j.gds.similarity.filteredknn;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.knn.ImmutableKnnContext;
import org.neo4j.gds.similarity.knn.KnnContext;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class FilteredKnnTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a { knn: 1.2, prop: 1.0 } )" +
        ", (b { knn: 1.1, prop: 5.0 } )" +
        ", (c { knn: 42.0, prop: 10.0 } )";
    @Inject
    private Graph graph;

    @GdlGraph(graphNamePrefix = "simThreshold")
    private static final String nodeCreateQuery =
        "CREATE " +
        "  (alice:Person {age: 23})" +
        " ,(carol:Person {age: 24})" +
        " ,(eve:Person {age: 34})" +
        " ,(bob:Person {age: 30})";
    @Inject
    private TestGraph simThresholdGraph;

    @GdlGraph(graphNamePrefix = "multPropMissing")
    private static final String nodeCreateMultipleQuery =
        "CREATE " +
        "  (a: P1 {prop1: 1.0, prop2: 5.0})" +
        " ,(b: P1 {prop2 : 5.0})" +
        " ,(c: P1 {prop2 : 10.0})" +
        " ,(d: P1 {prop3 : 10.0})";
    @Inject
    private TestGraph multPropMissingGraph;


    @Inject
    private IdFunction idFunction;

    @Test
    void shouldRunJustLikeKnnWhenYouDoNotSpecifySourceNodeFilterOrTargetNodeFilter() {
        var knnConfig = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
            .concurrency(1)
            .randomSeed(19L)
            .topK(1)
            .build();
        var knnContext = ImmutableKnnContext.builder().build();

        var knn = FilteredKnn.create(graph, knnConfig, knnContext);
        var result = knn.compute();

        assertThat(result).isNotNull();
        assertThat(result.similarityResultStream().count()).isEqualTo(3);

        long nodeAId = idFunction.of("a");
        long nodeBId = idFunction.of("b");
        long nodeCId = idFunction.of("c");

        assertCorrectNeighborList(result, nodeAId, nodeBId);
        assertCorrectNeighborList(result, nodeBId, nodeAId);
        assertCorrectNeighborList(result, nodeCId, nodeAId);
    }

    private void assertCorrectNeighborList(FilteredKnnResult result, long nodeId, long... expectedNeighbors) {
        List<SimilarityResult> similarityResults = result.similarityResultStream()
            .filter(sr -> sr.sourceNodeId() == nodeId)
            .collect(Collectors.toList());

        assertThat(similarityResults)
            .isSortedAccordingTo(Comparator.comparingDouble((s) -> -s.similarity));

        var justNeighbours = similarityResults.stream().mapToLong(SimilarityResult::targetNodeId).toArray();
        assertThat(justNeighbours)
            .doesNotContain(nodeId)
            .containsAnyOf(expectedNeighbors)
            .doesNotHaveDuplicates()
            .hasSizeLessThanOrEqualTo(expectedNeighbors.length);
    }

    @Nested
    class SourceNodeFilterTest {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a { knn: 1.2 } )" +
            ", (b { knn: 1.1 } )" +
            ", (c { knn: 2.1 } )" +
            ", (d { knn: 3.1 } )" +
            ", (e { knn: 4.1 } )";

        @Test
        void shouldOnlyProduceResultsForFilteredSourceNode() {
            var filteredSourceNode = idFunction.of("a");
            var config = FilteredKnnBaseConfigImpl.builder()
                .nodeProperties(List.of("knn"))
                .topK(3)
                .randomJoins(0)
                .maxIterations(1)
                .randomSeed(20L)
                .concurrency(1)
                .sourceNodeFilter(filteredSourceNode)
                .build();
            var knnContext = KnnContext.empty();
            var knn = FilteredKnn.create(graph, config, knnContext);
            var result = knn.compute();

            assertThat(result.similarityResultStream()
                .map(sr -> sr.node1)
                .collect(Collectors.toSet())
            ).isEqualTo(Set.of(filteredSourceNode));
        }

        @Test
        void shouldOnlyProduceResultsForMultipleFilteredSourceNode() {
            var filteredNode1 = idFunction.of("a");
            var filteredNode2 = idFunction.of("b");
            var config = FilteredKnnBaseConfigImpl.builder()
                .nodeProperties("knn")
                .topK(3)
                .randomJoins(0)
                .maxIterations(1)
                .randomSeed(20L)
                .concurrency(1)
                .sourceNodeFilter(List.of(filteredNode1, filteredNode2))
                .build();
            var knnContext = KnnContext.empty();
            var knn = FilteredKnn.create(graph, config, knnContext);
            var result = knn.compute();

            assertThat(result.similarityResultStream()
                .map(sr -> sr.node1)
                .collect(Collectors.toSet())
            ).isEqualTo(Set.of(filteredNode1, filteredNode2));
        }
    }
}
