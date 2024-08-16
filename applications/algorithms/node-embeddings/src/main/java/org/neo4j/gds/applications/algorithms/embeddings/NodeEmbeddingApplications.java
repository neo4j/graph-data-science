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

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;

public final class NodeEmbeddingApplications {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationMode;
    private final NodeEmbeddingAlgorithmsMutateModeBusinessFacade mutateMode;
    private final NodeEmbeddingAlgorithmsStatsModeBusinessFacade statsMode;
    private final NodeEmbeddingAlgorithmsStreamModeBusinessFacade streamMode;
    private final NodeEmbeddingAlgorithmsTrainModeBusinessFacade trainMode;
    private final NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeMode;

    private NodeEmbeddingApplications(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationMode,
        NodeEmbeddingAlgorithmsMutateModeBusinessFacade mutateMode,
        NodeEmbeddingAlgorithmsStatsModeBusinessFacade statsMode,
        NodeEmbeddingAlgorithmsStreamModeBusinessFacade streamMode,
        NodeEmbeddingAlgorithmsTrainModeBusinessFacade trainMode,
        NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeMode
    ) {
        this.estimationMode = estimationMode;
        this.mutateMode = mutateMode;
        this.statsMode = statsMode;
        this.streamMode = streamMode;
        this.trainMode = trainMode;
        this.writeMode = writeMode;
    }

    public static NodeEmbeddingApplications create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodeProperty mutateNodeProperty,
        ModelCatalog modelCatalog,
        ModelRepository modelRepository
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

        var graphSageAlgorithmProcessing = new GraphSageAlgorithmProcessing(
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience,
            graphSageModelCatalog
        );
        var node2VecAlgorithmProcessing = new Node2VecAlgorithmProcessing(
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience
        );

        var mutateMode = new NodeEmbeddingAlgorithmsMutateModeBusinessFacade(
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience,
            mutateNodeProperty,
            graphSageAlgorithmProcessing,
            node2VecAlgorithmProcessing
        );
        var statsMode = new NodeEmbeddingAlgorithmsStatsModeBusinessFacade(
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience
        );
        var streamMode = new NodeEmbeddingAlgorithmsStreamModeBusinessFacade(
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience,
            graphSageAlgorithmProcessing
        );
        var trainMode = new NodeEmbeddingAlgorithmsTrainModeBusinessFacade(
            graphSageModelCatalog,
            modelRepository,
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience
        );
        var writeMode = NodeEmbeddingAlgorithmsWriteModeBusinessFacade.create(
            log,
            requestScopedDependencies,
            writeContext,
            estimationMode,
            algorithms,
            algorithmProcessingTemplateConvenience,
            graphSageAlgorithmProcessing,
            node2VecAlgorithmProcessing
        );

        return new NodeEmbeddingApplications(estimationMode, mutateMode, statsMode, streamMode, trainMode, writeMode);
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

    public NodeEmbeddingAlgorithmsTrainModeBusinessFacade train() {
        return trainMode;
    }

    public NodeEmbeddingAlgorithmsWriteModeBusinessFacade write() {
        return writeMode;
    }
}
