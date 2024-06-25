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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.FastRP;

public class NodeEmbeddingAlgorithmsMutateModeBusinessFacade {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplate template;
    private final MutateNodeProperty mutateNodeProperty;

    public NodeEmbeddingAlgorithmsMutateModeBusinessFacade(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplate template,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.template = template;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT fastRP(
        GraphName graphName,
        FastRPMutateConfig configuration,
        ResultBuilder<FastRPMutateConfig, FastRPResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new FastRPMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            FastRP,
            () -> estimation.fastRP(configuration),
            graph -> algorithms.fastRP(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }
}
