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
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class PipelinesProcedureFacade {
    public static final String NO_VALUE = "__NO_VALUE";

    private final PipelineConfigurationParser pipelineConfigurationParser = new PipelineConfigurationParser();

    private final PipelineApplications pipelineApplications;

    PipelinesProcedureFacade(PipelineApplications pipelineApplications) {
        this.pipelineApplications = pipelineApplications;
    }

    public static PipelinesProcedureFacade create(PipelineRepository pipelineRepository, User user) {
        var pipelineApplications = new PipelineApplications(pipelineRepository, user);

        return new PipelinesProcedureFacade(pipelineApplications);
    }

    public Stream<NodePipelineInfoResult> addLogisticRegression(
        String pipelineName,
        Map<String, Object> configuration
    ) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseLogisticRegressionTrainerConfig(configuration),
            pipelineApplications::addTrainerConfiguration
        );
    }

    public Stream<NodePipelineInfoResult> addMLP(String pipelineName, Map<String, Object> configuration) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseMLPClassifierTrainConfig(configuration),
            pipelineApplications::addTrainerConfiguration
        );
    }

    public Stream<NodePipelineInfoResult> addNodeProperty(
        String pipelineNameAsString,
        String taskName,
        Map<String, Object> procedureConfig
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipeline = pipelineApplications.addNodeProperty(pipelineName, taskName, procedureConfig);

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    public Stream<NodePipelineInfoResult> addRandomForest(String pipelineName, Map<String, Object> configuration) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseRandomForestClassifierTrainerConfig(configuration),
            pipelineApplications::addTrainerConfiguration
        );
    }

    public Stream<NodePipelineInfoResult> configureAutoTuning(String pipelineName, Map<String, Object> configuration) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseAutoTuningConfig(configuration),
            pipelineApplications::configureAutoTuning
        );
    }

    public Stream<NodePipelineInfoResult> configureSplit(String pipelineName, Map<String, Object> configuration) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseNodePropertyPredictionSplitConfig(configuration),
            pipelineApplications::configureSplit
        );
    }

    public Stream<NodePipelineInfoResult> createPipeline(String pipelineNameAsString) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipeline = pipelineApplications.createNodeClassificationTrainingPipeline(pipelineName);

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    public Stream<PipelineCatalogResult> drop(
        String pipelineNameAsString,
        boolean failIfMissing
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        if (failIfMissing) {
            var result = pipelineApplications.dropAcceptingFailure(pipelineName);

            return Stream.of(PipelineCatalogResult.create(result, pipelineName.value));
        }

        var result = pipelineApplications.dropSilencingFailure(pipelineName);

        return Stream.ofNullable(result).map(pipeline -> PipelineCatalogResult.create(pipeline, pipelineName.value));
    }

    public Stream<PipelineExistsResult> exists(String pipelineNameAsString) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipelineType = pipelineApplications.exists(pipelineName);

        if (pipelineType.isEmpty()) return Stream.of(PipelineExistsResult.empty(pipelineName));

        var result = new PipelineExistsResult(pipelineName.value, pipelineType.get(), true);

        return Stream.of(result);
    }

    public Stream<PipelineCatalogResult> list(String pipelineNameAsString) {
        if (pipelineNameAsString == null || pipelineNameAsString.equals(NO_VALUE)) {
            var pipelineEntries = pipelineApplications.getAll();

            return pipelineEntries.map(
                entry -> PipelineCatalogResult.create(
                    entry.pipeline(),
                    entry.pipelineName()
                )
            );
        }

        var pipelineName = PipelineName.parse(pipelineNameAsString);

        Optional<TrainingPipeline<?>> pipeline = pipelineApplications.getSingle(pipelineName);

        if (pipeline.isEmpty()) return Stream.empty();

        var result = PipelineCatalogResult.create(pipeline.get(), pipelineName.value);

        return Stream.of(result);
    }

    public Stream<NodePipelineInfoResult> selectFeatures(String pipelineNameAsString, Object nodeFeatureStepsAsObject) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var nodeFeatureSteps = parseNodeProperties(nodeFeatureStepsAsObject);

        var pipeline = pipelineApplications.selectFeatures(pipelineName, nodeFeatureSteps);

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    private <CONFIGURATION> Stream<NodePipelineInfoResult> configure(
        String pipelineNameAsString,
        Supplier<CONFIGURATION> configurationSupplier,
        BiFunction<PipelineName, CONFIGURATION, NodeClassificationTrainingPipeline> configurationAction
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var configuration = configurationSupplier.get();

        var pipeline = configurationAction.apply(pipelineName, configuration);

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    private List<NodeFeatureStep> parseNodeProperties(Object nodeProperties) {
        if (nodeProperties instanceof String) return List.of(NodeFeatureStep.of((String) nodeProperties));

        if (nodeProperties instanceof List) {
            //noinspection rawtypes
            var propertiesList = (List) nodeProperties;

            var nodeFeatureSteps = new ArrayList<NodeFeatureStep>(propertiesList.size());

            for (Object o : propertiesList) {
                if (o instanceof String)
                    nodeFeatureSteps.add(NodeFeatureStep.of((String) o));
                else
                    throw new IllegalArgumentException("The list `nodeProperties` is required to contain only strings.");
            }

            return nodeFeatureSteps;
        }

        throw new IllegalArgumentException("The value of `nodeProperties` is required to be a list of strings.");
    }
}
