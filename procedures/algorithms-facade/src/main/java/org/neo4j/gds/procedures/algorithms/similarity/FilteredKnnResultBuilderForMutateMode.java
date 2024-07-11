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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnMutateConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;

import java.util.Map;
import java.util.Optional;

public class FilteredKnnResultBuilderForMutateMode implements ResultBuilder<FilteredKnnMutateConfig, FilteredKnnResult, KnnMutateResult, Pair<RelationshipsWritten, Map<String, Object>>> {
    /**
     * @param metadata number of relationships written, and the similarity distribution
     */
    @Override
    public KnnMutateResult build(
        Graph graph,
        GraphStore graphStore,
        FilteredKnnMutateConfig configuration,
        Optional<FilteredKnnResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Pair<RelationshipsWritten, Map<String, Object>>> metadata
    ) {
        var configurationMap = configuration.toMap();

        return result.map(r -> KnnMutateResult.create(
            timings,
            configurationMap,
            metadata.orElseThrow().getLeft(),
            metadata.orElseThrow().getRight(),
            r.nodesCompared(),
            r.didConverge(),
            r.ranIterations(),
            r.nodePairsConsidered()
        )).orElseGet(() -> KnnMutateResult.emptyFrom(timings, configurationMap));
    }
}
