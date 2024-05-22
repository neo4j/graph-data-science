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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.similarity.filtering.NodeFilterSpecFactory;
import org.neo4j.gds.similarity.knn.KnnContext;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
public class FilteredKnnIdMappingTest {

    @GdlGraph(idOffset = 4242)
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a { knn: 1.2})" +
            ", (b { knn: 1.1})" +
            ", (c { knn: 2.1})" +
            ", (d { knn: 3.1})" +
            ", (e { knn: 4.1})";

    @Inject
    private Graph graph;
    @Test
    void shouldIdMapTheSourceNodeFilter() {


        var lowestOriginalId = LongStream.range(0, graph.nodeCount()).map(graph::toOriginalNodeId).min().orElse(-1);
        assertThat(lowestOriginalId).isPositive();

        var config = FilteredKnnBaseConfigImpl.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
            .topK(3)
            .randomJoins(0)
            .maxIterations(1)
            .randomSeed(20L)
            .concurrency(1)
            .sourceNodeFilter(NodeFilterSpecFactory.create(lowestOriginalId))
            .build();

        var knn = FilteredKnn.createWithoutSeeding(graph, config, KnnContext.empty(), TerminationFlag.RUNNING_TRUE);

        var result = knn.compute();

        // filtering on the lowest Neo ID means all resulting similarity relationships have source node 0
        var sourceNodesInResult = result
            .similarityResultStream()
            .map(res -> res.node1)
            .map(graph::toOriginalNodeId)
            .collect(Collectors.<Long>toSet());
        assertThat(sourceNodesInResult).containsExactly(lowestOriginalId);
        
    }
}
