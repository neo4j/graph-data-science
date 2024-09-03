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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.api.User;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.pipeline.AutoTuningConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

class PipelineApplications {
    private final PipelineRepository pipelineRepository;
    private final User user;

    PipelineApplications(PipelineRepository pipelineRepository, User user) {
        this.pipelineRepository = pipelineRepository;
        this.user = user;
    }

    NodeClassificationTrainingPipeline addNodeProperty(
        PipelineName pipelineName,
        String taskName,
        Map<String, Object> procedureConfig
    ) {
        var pipeline = pipelineRepository.getNodeClassificationTrainingPipeline(user, pipelineName);

        var nodePropertyStep = NodePropertyStepFactory.createNodePropertyStep(taskName, procedureConfig);

        pipeline.addNodePropertyStep(nodePropertyStep);

        return pipeline;
    }

    NodeClassificationTrainingPipeline addTrainerConfiguration(
        PipelineName pipelineName,
        TunableTrainerConfig configuration
    ) {
        return configure(pipelineName, pipeline -> pipeline.addTrainerConfig(configuration));
    }

    NodeClassificationTrainingPipeline configureAutoTuning(PipelineName pipelineName, AutoTuningConfig configuration) {
        return configure(pipelineName, pipeline -> pipeline.setAutoTuningConfig(configuration));
    }

    NodeClassificationTrainingPipeline configureSplit(
        PipelineName pipelineName, NodePropertyPredictionSplitConfig configuration
    ) {
        return configure(pipelineName, pipeline -> pipeline.setSplitConfig(configuration));
    }

    NodeClassificationTrainingPipeline createNodeClassificationTrainingPipeline(PipelineName pipelineName) {
        return pipelineRepository.createNodeClassificationTrainingPipeline(user, pipelineName);
    }

    /**
     * Straight delegation, the store will throw an exception
     */
    TrainingPipeline<?> dropAcceptingFailure(PipelineName pipelineName) {
        return pipelineRepository.drop(user, pipelineName);
    }

    /**
     * Pre-check for existence, return null
     */
    TrainingPipeline<?> dropSilencingFailure(PipelineName pipelineName) {
        if (!pipelineRepository.exists(user, pipelineName)) return null;

        return pipelineRepository.drop(user, pipelineName);
    }

    /**
     * @return the pipeline type, if the pipeline exists. Bit of an overload :)
     */
    Optional<String> exists(PipelineName pipelineName) {
        var pipelineExists = pipelineRepository.exists(user, pipelineName);

        if (!pipelineExists) return Optional.empty();

        var pipelineType = pipelineRepository.getType(user, pipelineName);

        return Optional.of(pipelineType);
    }

    Stream<PipelineCatalog.PipelineCatalogEntry> getAll() {
        return pipelineRepository.getAll(user);
    }

    Optional<TrainingPipeline<?>> getSingle(PipelineName pipelineName) {
        return pipelineRepository.getSingle(user, pipelineName);
    }

    NodeClassificationTrainingPipeline selectFeatures(
        PipelineName pipelineName,
        Iterable<NodeFeatureStep> nodeFeatureSteps
    ) {
        var pipeline = pipelineRepository.getNodeClassificationTrainingPipeline(user, pipelineName);

        for (NodeFeatureStep nodeFeatureStep : nodeFeatureSteps) {
            pipeline.addFeatureStep(nodeFeatureStep);
        }

        return pipeline;
    }

    private NodeClassificationTrainingPipeline configure(
        PipelineName pipelineName,
        Consumer<NodeClassificationTrainingPipeline> configurationAction
    ) {
        var pipeline = pipelineRepository.getNodeClassificationTrainingPipeline(
            user,
            pipelineName
        );

        configurationAction.accept(pipeline);

        return pipeline;
    }
}
