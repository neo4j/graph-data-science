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
package org.neo4j.gds.applications.algorithms.similarity;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.similarity.SimilarityGraph;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.TopKSimilarityGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

class SimilarityWrite {
    private final SimilarityResultStreamDelegate similarityResultStreamDelegate = new SimilarityResultStreamDelegate();

    private final WriteRelationshipService writeRelationshipService;

    SimilarityWrite(WriteRelationshipService writeRelationshipService) {
        this.writeRelationshipService = writeRelationshipService;
    }

    Pair<RelationshipsWritten, Map<String, Object>> execute(
        Graph graph,
        GraphStore graphStore,
        ConcurrencyConfig concurrencyConfiguration,
        WritePropertyConfig writePropertyConfiguration,
        WriteRelationshipConfig writeRelationshipConfiguration,
        Optional<ResultStore> resultStore,
        Stream<SimilarityResult> similarityResultStream,
        Label label,
        JobId jobId,
        boolean shouldComputeDistribution,
        TerminationFlag terminationFlag
    ) {
        var similarityGraphResult = similarityResultStreamDelegate.computeSimilarityGraphBuildResult(
            graph,
            concurrencyConfiguration.concurrency(),
            similarityResultStream,
            shouldComputeDistribution,
            terminationFlag
        );

        return execute(
            graphStore,
            writePropertyConfiguration,
            writeRelationshipConfiguration,
            resultStore,
            label,
            similarityGraphResult.graph(),
            jobId
        );
    }

    Pair<RelationshipsWritten, Map<String, Object>> execute(
        GraphStore graphStore,
        WritePropertyConfig writePropertyConfiguration,
        WriteRelationshipConfig writeRelationshipConfiguration,
        Optional<ResultStore> resultStore,
        Label label,
        SimilarityGraph similarityGraph,
        JobId jobId
    ) {

        var rootIdMap = similarityGraph instanceof TopKSimilarityGraph
            ? similarityGraph
            : graphStore.nodes();


        var relationshipsWritten = writeRelationshipService.writeFromGraph(
            writeRelationshipConfiguration.writeRelationshipType(),
            writePropertyConfiguration.writeProperty(),
            similarityGraph,
            rootIdMap,
            label.asString(),
            resultStore,
            jobId
        );


        return Pair.of(relationshipsWritten, similarityGraph.similarityDistribution());
    }
}
