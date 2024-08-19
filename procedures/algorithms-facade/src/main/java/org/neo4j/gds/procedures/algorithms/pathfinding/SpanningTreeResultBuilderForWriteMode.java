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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;

import java.util.Optional;

class SpanningTreeResultBuilderForWriteMode implements ResultBuilder<SpanningTreeWriteConfig, SpanningTree, SpanningTreeWriteResult, RelationshipsWritten> {
    @Override
    public SpanningTreeWriteResult build(
        Graph graph,
        GraphStore graphStore,
        SpanningTreeWriteConfig configuration,
        Optional<SpanningTree> result,
        AlgorithmProcessingTimings timings,
        Optional<RelationshipsWritten> metadata
    ) {
        var builder = new SpanningTreeWriteResult.Builder();

        if (result.isEmpty()) {
            return builder.build();
        }

        var spanningTree = result.get();

        builder
            .withEffectiveNodeCount(spanningTree.effectiveNodeCount())
            .withTotalWeight(spanningTree.totalWeight());

        builder.withComputeMillis(timings.computeMillis);
        builder.withPreProcessingMillis(timings.preProcessingMillis);
        builder.withWriteMillis(timings.mutateOrWriteMillis);

        metadata.ifPresent(rw -> builder.withRelationshipsWritten(rw.value()));

        builder.withConfig(configuration);

        return builder.build();
    }
}
