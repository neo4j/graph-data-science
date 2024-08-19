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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateOrWriteStep;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Node2Vec;

/**
 * This encapsulates applying the validation hook, which we need to do across modes
 */
public class Node2VecAlgorithmProcessing {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    public Node2VecAlgorithmProcessing(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    <CONFIGURATION extends Node2VecBaseConfig, RESULT, MUTATE_OR_WRITE_METADATA> RESULT process(
        GraphName graphName,
        CONFIGURATION configuration,
        MutateOrWriteStep<Node2VecResult, MUTATE_OR_WRITE_METADATA> mutateOrWriteStep,
        ResultBuilder<CONFIGURATION, Node2VecResult, RESULT, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        var validationHook = new Node2VecValidationHook(configuration);

        return algorithmProcessingTemplateConvenience.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.of(List.of(validationHook)),
            Node2Vec,
            () -> estimationFacade.node2Vec(configuration),
            (graph, __) -> algorithms.node2Vec(graph, configuration),
            mutateOrWriteStep,
            resultBuilder
        );
    }
}
