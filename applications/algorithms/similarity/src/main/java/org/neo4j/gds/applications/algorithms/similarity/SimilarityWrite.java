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
import org.neo4j.gds.algorithms.similarity.SimilaritySummaryBuilder;
import org.neo4j.gds.algorithms.similarity.WriteRelationshipService;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.WriteConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResult;

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
        WriteConfig writeConfiguration,
        WritePropertyConfig writePropertyConfiguration,
        WriteRelationshipConfig writeRelationshipConfiguration,
        boolean shouldComputeSimilarityDistribution,
        Optional<ResultStore> resultStore,
        Stream<SimilarityResult> similarityResultStream,
        LabelForProgressTracking label,
        JobId jobId
    ) {
        var similarityGraphResult = similarityResultStreamDelegate.computeSimilarityGraph(
            graph,
            concurrencyConfiguration.concurrency(),
            similarityResultStream
        );

        return execute(
            graphStore,
            writeConfiguration,
            writePropertyConfiguration,
            writeRelationshipConfiguration,
            shouldComputeSimilarityDistribution,
            resultStore,
            label,
            similarityGraphResult,
            jobId
        );
    }

    Pair<RelationshipsWritten, Map<String, Object>> execute(
        GraphStore graphStore,
        WriteConfig writeConfiguration,
        WritePropertyConfig writePropertyConfiguration,
        WriteRelationshipConfig writeRelationshipConfiguration,
        boolean shouldComputeSimilarityDistribution,
        Optional<ResultStore> resultStore,
        LabelForProgressTracking label,
        SimilarityGraphResult similarityGraphResult,
        JobId jobId
    ) {
        var similarityGraph = similarityGraphResult.similarityGraph();

        var rootIdMap = similarityGraphResult.isTopKGraph()
            ? similarityGraph
            : graphStore.nodes();

        var similarityDistributionBuilder = SimilaritySummaryBuilder.of(shouldComputeSimilarityDistribution);

        var writeResult = writeRelationshipService.write(
            writeRelationshipConfiguration.writeRelationshipType(),
            writePropertyConfiguration.writeProperty(),
            similarityGraph,
            graphStore,
            rootIdMap,
            label.value,
            writeConfiguration.arrowConnectionInfo(),
            resultStore,
            similarityDistributionBuilder.similarityConsumer(),
            jobId
        );

        var similaritySummary = similarityDistributionBuilder.similaritySummary();

        return Pair.of(new RelationshipsWritten(writeResult.relationshipsWritten()), similaritySummary);
    }
}
