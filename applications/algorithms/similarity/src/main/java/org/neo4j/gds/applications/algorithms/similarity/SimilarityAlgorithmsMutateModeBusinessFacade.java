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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnMutateConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityMutateConfig;
import org.neo4j.gds.similarity.knn.KnnMutateConfig;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.FilteredKNN;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.FilteredNodeSimilarity;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.KNN;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.NodeSimilarity;

public class SimilarityAlgorithmsMutateModeBusinessFacade {
    private final Log log;
    private final SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final SimilarityAlgorithms similarityAlgorithms;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;

    public SimilarityAlgorithmsMutateModeBusinessFacade(
        Log log, SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        SimilarityAlgorithms similarityAlgorithms,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        this.log = log;
        this.estimationFacade = estimationFacade;
        this.similarityAlgorithms = similarityAlgorithms;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
    }

    public <RESULT> RESULT filteredKnn(
        GraphName graphName,
        FilteredKnnMutateConfig configuration,
        ResultBuilder<FilteredKnnMutateConfig, FilteredKnnResult, RESULT, Pair<RelationshipsWritten, Map<String, Object>>> resultBuilder,
        boolean shouldComputeSimilarityDistribution
    ) {
        var mutateStep = FilteredKnnMutateStep.create(log, configuration, shouldComputeSimilarityDistribution);

        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            FilteredKNN,
            () -> estimationFacade.filteredKnn(configuration),
            graph -> similarityAlgorithms.filteredKnn(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT filteredNodeSimilarity(
        GraphName graphName,
        FilteredNodeSimilarityMutateConfig configuration,
        ResultBuilder<FilteredNodeSimilarityMutateConfig, NodeSimilarityResult, RESULT, Pair<RelationshipsWritten, Map<String, Object>>> resultBuilder,
        boolean shouldComputeSimilarityDistribution
    ) {
        var mutateStep = FilteredNodeSimilarityMutateStep.create(
            log,
            configuration,
            shouldComputeSimilarityDistribution
        );

        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            FilteredNodeSimilarity,
            () -> estimationFacade.filteredNodeSimilarity(configuration),
            graph -> similarityAlgorithms.filteredNodeSimilarity(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT knn(
        GraphName graphName,
        KnnMutateConfig configuration,
        ResultBuilder<KnnMutateConfig, KnnResult, RESULT, Pair<RelationshipsWritten, Map<String, Object>>> resultBuilder,
        boolean shouldComputeSimilarityDistribution
    ) {
        var mutateStep = KnnMutateStep.create(log, configuration, shouldComputeSimilarityDistribution);

        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            KNN,
            () -> estimationFacade.knn(configuration),
            graph -> similarityAlgorithms.knn(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT nodeSimilarity(
        GraphName graphName,
        NodeSimilarityMutateConfig configuration,
        ResultBuilder<NodeSimilarityMutateConfig, NodeSimilarityResult, RESULT, Pair<RelationshipsWritten, Map<String, Object>>> resultBuilder,
        boolean shouldComputeSimilarityDistribution
    ) {
        var mutateStep = NodeSimilarityMutateStep.create(log, configuration, shouldComputeSimilarityDistribution);

        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            NodeSimilarity,
            () -> estimationFacade.nodeSimilarity(configuration),
            graph -> similarityAlgorithms.nodeSimilarity(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }
}
