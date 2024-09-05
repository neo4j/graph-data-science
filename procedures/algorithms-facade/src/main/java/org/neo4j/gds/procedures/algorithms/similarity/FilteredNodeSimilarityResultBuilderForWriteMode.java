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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityWriteConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

class FilteredNodeSimilarityResultBuilderForWriteMode implements ResultBuilder<FilteredNodeSimilarityWriteConfig, NodeSimilarityResult, Stream<SimilarityWriteResult>, Pair<RelationshipsWritten, Map<String, Object>>> {
    private final GenericNodeSimilarityResultBuilderForWriteMode genericResultBuilder = new GenericNodeSimilarityResultBuilderForWriteMode();

    @Override
    public Stream<SimilarityWriteResult> build(
        Graph graph,
        FilteredNodeSimilarityWriteConfig configuration,
        Optional<NodeSimilarityResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Pair<RelationshipsWritten, Map<String, Object>>> metadata
    ) {
        return genericResultBuilder.build(
            configuration.toMap(),
            result,
            timings,
            metadata
        );
    }
}
