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
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public final class PipelinesProcedureFacade {
    public static final String NO_VALUE = "__NO_VALUE";

    private final PipelineApplications pipelineApplications;
    private final User user;

    private PipelinesProcedureFacade(PipelineApplications pipelineApplications, User user) {
        this.pipelineApplications = pipelineApplications;
        this.user = user;
    }

    public static PipelinesProcedureFacade create(PipelineRepository pipelineRepository, User user) {
        var pipelineApplications = new PipelineApplications(pipelineRepository, user);

        return new PipelinesProcedureFacade(pipelineApplications, user);
    }

    public Stream<NodePipelineInfoResult> addNodeProperty(
        String pipelineName,
        String taskName,
        Map<String, Object> procedureConfig
    ) {
        var pipeline = PipelineCatalog.getTyped(
            user.getUsername(),
            pipelineName,
            NodeClassificationTrainingPipeline.class
        );

        var nodePropertyStep = NodePropertyStepFactory.createNodePropertyStep(taskName, procedureConfig);

        pipeline.addNodePropertyStep(nodePropertyStep);

        return Stream.of(new NodePipelineInfoResult(pipelineName, pipeline));
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

        //noinspection OptionalIsPresent
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

        //noinspection OptionalIsPresent
        if (pipeline.isEmpty()) return Stream.empty();

        var result = PipelineCatalogResult.create(pipeline.get(), pipelineName.value);

        return Stream.of(result);
    }

    public Stream<NodePipelineInfoResult> selectFeatures(String pipelineName, Object nodeProperties) {
        var result = selectFeatures(
            user.getUsername(),
            pipelineName,
            nodeProperties
        );

        return Stream.of(result);
    }

    private NodePipelineInfoResult selectFeatures(
        String username,
        String pipelineName,
        Object nodeProperties
    ) {
        var pipeline = PipelineCatalog.getTyped(username, pipelineName, NodeClassificationTrainingPipeline.class);

        if (nodeProperties instanceof String) {
            pipeline.addFeatureStep(NodeFeatureStep.of((String) nodeProperties));
        } else if (nodeProperties instanceof List) {
            var propertiesList = (List) nodeProperties;
            for (Object o : propertiesList) {
                if (!(o instanceof String)) {
                    throw new IllegalArgumentException("The list `nodeProperties` is required to contain only strings.");
                }

                pipeline.addFeatureStep(NodeFeatureStep.of((String) o));
            }
        } else {
            throw new IllegalArgumentException("The value of `nodeProperties` is required to be a list of strings.");
        }

        return new NodePipelineInfoResult(pipelineName, pipeline);
    }
}
