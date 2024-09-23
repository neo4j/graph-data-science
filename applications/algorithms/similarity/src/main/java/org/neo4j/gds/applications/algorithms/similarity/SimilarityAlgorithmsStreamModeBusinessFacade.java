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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStreamConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStreamConfig;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.KnnStreamConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStreamConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.FilteredKNN;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.FilteredNodeSimilarity;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.KNN;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.NodeSimilarity;

public class SimilarityAlgorithmsStreamModeBusinessFacade {
    private final SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final SimilarityAlgorithms similarityAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    SimilarityAlgorithmsStreamModeBusinessFacade(
        SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        SimilarityAlgorithms similarityAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.estimationFacade = estimationFacade;
        this.similarityAlgorithms = similarityAlgorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> Stream<RESULT> filteredKnn(
        GraphName graphName,
        FilteredKnnStreamConfig configuration,
        StreamResultBuilder<FilteredKnnResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            FilteredKNN,
            () -> estimationFacade.filteredKnn(configuration),
            (graph, __) -> similarityAlgorithms.filteredKnn(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> filteredNodeSimilarity(
        GraphName graphName,
        FilteredNodeSimilarityStreamConfig configuration,
        StreamResultBuilder<NodeSimilarityResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            FilteredNodeSimilarity,
            () -> estimationFacade.filteredNodeSimilarity(configuration),
            (graph, __) -> similarityAlgorithms.filteredNodeSimilarity(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> knn(
        GraphName graphName,
        KnnStreamConfig configuration,
        StreamResultBuilder<KnnResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            KNN,
            () -> estimationFacade.knn(configuration),
            (graph, __) -> similarityAlgorithms.knn(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> nodeSimilarity(
        GraphName graphName,
        NodeSimilarityStreamConfig configuration,
        StreamResultBuilder<NodeSimilarityResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            NodeSimilarity,
            () -> estimationFacade.nodeSimilarity(configuration),
            (graph, __) -> similarityAlgorithms.nodeSimilarity(graph, configuration),
            resultBuilder
        );
    }
}
