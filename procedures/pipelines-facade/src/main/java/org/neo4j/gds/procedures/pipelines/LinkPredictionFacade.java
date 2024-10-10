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
import org.neo4j.gds.ml.pipeline.TrainingPipeline;

import java.util.Map;
import java.util.stream.Stream;

public final class LinkPredictionFacade {
    private final Configurer configurer;

    private final PipelineConfigurationParser pipelineConfigurationParser;
    private final PipelineApplications pipelineApplications;

    private LinkPredictionFacade(
        Configurer configurer, PipelineConfigurationParser pipelineConfigurationParser,
        PipelineApplications pipelineApplications
    ) {
        this.configurer = configurer;
        this.pipelineConfigurationParser = pipelineConfigurationParser;
        this.pipelineApplications = pipelineApplications;
    }

    static LinkPredictionFacade create(
        User user,
        PipelineConfigurationParser pipelineConfigurationParser,
        PipelineApplications pipelineApplications,
        PipelineRepository pipelineRepository
    ) {
        var configurer = new Configurer(pipelineRepository, user);

        return new LinkPredictionFacade(configurer, pipelineConfigurationParser, pipelineApplications);
    }

    public Stream<PipelineInfoResult> addFeature(
        String pipelineNameAsString,
        String featureType,
        Map<String, Object> rawConfiguration
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);
        var configuration = pipelineConfigurationParser.parseLinkFeatureStepConfiguration(rawConfiguration);

        var pipeline = pipelineApplications.addFeature(pipelineName, featureType, configuration);

        var result = PipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    public Stream<PipelineInfoResult> addLogisticRegression(String pipelineName, Map<String, Object> configuration) {
        return configurer.configureLinkPredictionTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseLogisticRegressionTrainerConfig(configuration),
            TrainingPipeline::addTrainerConfig
        );
    }

    public Stream<PipelineInfoResult> addNodeProperty(
        String pipelineNameAsString,
        String taskName,
        Map<String, Object> procedureConfig
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipeline = pipelineApplications.addNodePropertyToLinkPredictionPipeline(
            pipelineName,
            taskName,
            procedureConfig
        );

        var result = PipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }
}
