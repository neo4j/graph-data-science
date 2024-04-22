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
package org.neo4j.gds.procedures.algorithms.similarity;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStreamConfig;

import java.util.Optional;
import java.util.stream.Stream;

class FilteredKnnResultBuilderForStreamMode implements ResultBuilder<FilteredKnnStreamConfig, FilteredKnnResult, Stream<SimilarityResult>, Void> {
    @Override
    public Stream<SimilarityResult> build(
        Graph graph,
        GraphStore graphStore,
        FilteredKnnStreamConfig configuration,
        Optional<FilteredKnnResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Void> unused
    ) {
        if (result.isEmpty()) return Stream.empty();

        //noinspection SimplifyStreamApiCallChains
        return result.get().similarityResultStream().map(sr -> {
            sr.node1 = graph.toOriginalNodeId(sr.node1);
            sr.node2 = graph.toOriginalNodeId(sr.node2);
            return sr;
        });
    }
}
