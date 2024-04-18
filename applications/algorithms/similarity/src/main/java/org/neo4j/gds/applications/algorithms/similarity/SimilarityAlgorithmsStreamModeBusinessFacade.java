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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.KnnStreamConfig;

import java.util.Optional;

public class SimilarityAlgorithmsStreamModeBusinessFacade {
    private final SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final SimilarityAlgorithms similarityAlgorithms;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;

    public SimilarityAlgorithmsStreamModeBusinessFacade(
        SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        SimilarityAlgorithms similarityAlgorithms,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        this.estimationFacade = estimationFacade;
        this.similarityAlgorithms = similarityAlgorithms;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
    }

    public <RESULT> RESULT knn(
        GraphName graphName,
        KnnStreamConfig configuration,
        ResultBuilder<KnnStreamConfig, KnnResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            AlgorithmLabels.KNN,
            () -> estimationFacade.knn(configuration),
            graph -> similarityAlgorithms.knn(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }
}
