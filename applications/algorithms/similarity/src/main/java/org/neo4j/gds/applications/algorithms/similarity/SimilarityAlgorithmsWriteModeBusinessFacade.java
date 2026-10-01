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
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.WriteResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.WriteSideEffect;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnWriteConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityWriteConfig;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.KnnWriteConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityWriteConfig;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.Optional;

public class SimilarityAlgorithmsWriteModeBusinessFacade {
    private final WriteRelationshipService writeRelationshipService;
    private final TerminationFlag terminationFlag;
    private final InstrumentedSimilarityAlgorithms instrumentedSimilarityAlgorithms;
    private final Synchroniser synchroniser;

    SimilarityAlgorithmsWriteModeBusinessFacade(
        WriteRelationshipService writeRelationshipService,
        TerminationFlag terminationFlag,
        InstrumentedSimilarityAlgorithms instrumentedSimilarityAlgorithms,
        Synchroniser synchroniser
    ) {
        this.writeRelationshipService = writeRelationshipService;
        this.terminationFlag = terminationFlag;
        this.instrumentedSimilarityAlgorithms = instrumentedSimilarityAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> RESULT filteredKnn(
        GraphName graphName,
        FilteredKnnWriteConfig configuration,
        ResultBuilder<FilteredKnnWriteConfig, FilteredKnnResult, RESULT, Pair<RelationshipsWritten, Map<String, Object>>> resultBuilder,
        boolean shouldComputeSimilarityDistribution
    ) {
        var writeStep = FilteredKnnWriteStep.create(
            configuration,
            shouldComputeSimilarityDistribution,
            writeRelationshipService,
            terminationFlag
        );

        return synchroniser.synchronise(() -> instrumentedSimilarityAlgorithms.filteredKnn(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT filteredNodeSimilarity(
        GraphName graphName,
        FilteredNodeSimilarityWriteConfig configuration,
        ResultBuilder<FilteredNodeSimilarityWriteConfig, NodeSimilarityResult, RESULT, Pair<RelationshipsWritten, Map<String, Object>>> resultBuilder,
        boolean shouldComputeSimilarityDistribution
    ) {
        var writeStep = FilteredNodeSimilarityWriteStep.create(
            writeRelationshipService,
            configuration,
            terminationFlag,
            shouldComputeSimilarityDistribution
        );

        return synchroniser.synchronise(() -> instrumentedSimilarityAlgorithms.filteredNodeSimilarity(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT knn(
        GraphName graphName,
        KnnWriteConfig configuration,
        ResultBuilder<KnnWriteConfig, KnnResult, RESULT, Pair<RelationshipsWritten, Map<String, Object>>> resultBuilder,
        boolean shouldComputeSimilarityDistribution
    ) {
        var writeStep = KnnWriteStep.create(
            configuration,
            shouldComputeSimilarityDistribution,
            writeRelationshipService,
            terminationFlag
        );

        return synchroniser.synchronise(() -> instrumentedSimilarityAlgorithms.knn(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT nodeSimilarity(
        GraphName graphName,
        NodeSimilarityWriteConfig configuration,
        ResultBuilder<NodeSimilarityWriteConfig, NodeSimilarityResult, RESULT, Pair<RelationshipsWritten, Map<String, Object>>> resultBuilder,
        boolean shouldComputeSimilarityDistribution
    ) {
        var writeStep = NodeSimilarityWriteStep.create(
            writeRelationshipService,
            configuration,
            terminationFlag,
            shouldComputeSimilarityDistribution
        );

        return synchroniser.synchronise(() -> instrumentedSimilarityAlgorithms.nodeSimilarity(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }
}
