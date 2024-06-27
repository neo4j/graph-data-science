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
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.WriteToDatabase;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteConfig;
import org.neo4j.gds.logging.Log;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.FastRP;

public final class NodeEmbeddingAlgorithmsWriteModeBusinessFacade {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;
    private final WriteToDatabase writeToDatabase;

    private NodeEmbeddingAlgorithmsWriteModeBusinessFacade(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        WriteToDatabase writeToDatabase
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.writeToDatabase = writeToDatabase;
    }

    public static NodeEmbeddingAlgorithmsWriteModeBusinessFacade create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        var writeNodePropertyService = new WriteNodePropertyService(log, requestScopedDependencies, writeContext);
        var writeToDatabase = new WriteToDatabase(writeNodePropertyService);

        return new NodeEmbeddingAlgorithmsWriteModeBusinessFacade(
            estimationFacade,
            algorithms,
            algorithmProcessingTemplate,
            writeToDatabase
        );
    }

    public <RESULT> RESULT fastRP(
        GraphName graphName,
        FastRPWriteConfig configuration,
        ResultBuilder<FastRPWriteConfig, FastRPResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new FastRPWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            FastRP,
            () -> estimationFacade.fastRP(configuration),
            graph -> algorithms.fastRP(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }
}
