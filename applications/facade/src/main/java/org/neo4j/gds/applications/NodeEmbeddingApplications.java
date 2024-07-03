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
package org.neo4j.gds.applications;

import org.neo4j.gds.applications.algorithms.embeddings.GraphSageModelCatalog;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithms;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;

public final class NodeEmbeddingApplications {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationMode;
    private final NodeEmbeddingAlgorithmsMutateModeBusinessFacade mutateMode;
    private final NodeEmbeddingAlgorithmsStatsModeBusinessFacade statsMode;
    private final NodeEmbeddingAlgorithmsStreamModeBusinessFacade streamMode;
    private final NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeMode;

    private NodeEmbeddingApplications(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationMode,
        NodeEmbeddingAlgorithmsMutateModeBusinessFacade mutateMode,
        NodeEmbeddingAlgorithmsStatsModeBusinessFacade statsMode,
        NodeEmbeddingAlgorithmsStreamModeBusinessFacade streamMode,
        NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeMode
    ) {
        this.estimationMode = estimationMode;
        this.mutateMode = mutateMode;
        this.statsMode = statsMode;
        this.streamMode = streamMode;
        this.writeMode = writeMode;
    }

    static NodeEmbeddingApplications create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodeProperty mutateNodeProperty,
        ModelCatalog modelCatalog
    ) {
        var graphSageModelCatalog = new GraphSageModelCatalog(modelCatalog);

        var algorithms = new NodeEmbeddingAlgorithms(
            graphSageModelCatalog,
            progressTrackerCreator,
            requestScopedDependencies.getTerminationFlag()
        );

        var estimationMode = new NodeEmbeddingAlgorithmsEstimationModeBusinessFacade(
            graphSageModelCatalog,
            algorithmEstimationTemplate
        );
        var mutateMode = new NodeEmbeddingAlgorithmsMutateModeBusinessFacade(
            graphSageModelCatalog,
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience,
            mutateNodeProperty
        );
        var statsMode = new NodeEmbeddingAlgorithmsStatsModeBusinessFacade(
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience
        );
        var streamMode = new NodeEmbeddingAlgorithmsStreamModeBusinessFacade(
            graphSageModelCatalog,
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience
        );
        var writeMode = NodeEmbeddingAlgorithmsWriteModeBusinessFacade.create(
            log,
            graphSageModelCatalog,
            requestScopedDependencies,
            writeContext,
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience
        );

        return new NodeEmbeddingApplications(estimationMode, mutateMode, statsMode, streamMode, writeMode);
    }

    public NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimationMode;
    }

    public NodeEmbeddingAlgorithmsMutateModeBusinessFacade mutate() {
        return mutateMode;
    }

    public NodeEmbeddingAlgorithmsStatsModeBusinessFacade stats() {
        return statsMode;
    }

    public NodeEmbeddingAlgorithmsStreamModeBusinessFacade stream() {
        return streamMode;
    }

    public NodeEmbeddingAlgorithmsWriteModeBusinessFacade write() {
        return writeMode;
    }
}
