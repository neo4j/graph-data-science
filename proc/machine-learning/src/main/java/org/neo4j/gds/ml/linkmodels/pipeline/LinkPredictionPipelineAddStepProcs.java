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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStepFactory;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.LinkFeatureStepConfigurationImpl;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.config.RelationshipWeightConfig.RELATIONSHIP_WEIGHT_PROPERTY;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig.CONTEXT_RELATIONSHIP_TYPES;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class LinkPredictionPipelineAddStepProcs extends BaseProc {

    @Procedure(name = "gds.beta.pipeline.linkPrediction.addNodeProperty", mode = READ)
    @Description("Add a node property step to an existing link prediction pipeline.")
    public Stream<PipelineInfoResult> addNodeProperty(
        @Name("pipelineName") String pipelineName,
        @Name("procedureName") String taskName,
        @Name("procedureConfiguration") Map<String, Object> procedureConfig
    ) {
        var pipeline = PipelineCatalog.getTyped(username(), pipelineName, LinkPredictionTrainingPipeline.class);
        validateRelationshipProperty(pipeline, procedureConfig);

        validateContextRelationshipTypesStar(procedureConfig);

        pipeline.addNodePropertyStep(NodePropertyStepFactory.createNodePropertyStep(taskName, procedureConfig));

        return Stream.of(new PipelineInfoResult(pipelineName, pipeline));
    }

    @Procedure(name = "gds.beta.pipeline.linkPrediction.addFeature", mode = READ)
    @Description("Add a feature step to an existing link prediction pipeline.")
    public Stream<PipelineInfoResult> addFeature(
        @Name("pipelineName") String pipelineName,
        @Name("featureType") String featureType,
        @Name("configuration") Map<String, Object> config
    ) {
        var pipeline = PipelineCatalog.getTyped(username(), pipelineName, LinkPredictionTrainingPipeline.class);

        var parsedConfig = new LinkFeatureStepConfigurationImpl(CypherMapWrapper.create(config));

        pipeline.addFeatureStep(LinkFeatureStepFactory.create(featureType, parsedConfig));

        return Stream.of(new PipelineInfoResult(pipelineName, pipeline));
    }

    // check if adding would result in more than one relationshipWeightProperty
    private void validateRelationshipProperty(
        LinkPredictionTrainingPipeline pipeline,
        Map<String, Object> procedureConfig
    ) {
        if (!procedureConfig.containsKey(RELATIONSHIP_WEIGHT_PROPERTY)) return;
        var maybeRelationshipProperty = pipeline.relationshipWeightProperty(executionContext());
        if (maybeRelationshipProperty.isEmpty()) return;
        var relationshipProperty = maybeRelationshipProperty.get();
        var property = (String) procedureConfig.get(RELATIONSHIP_WEIGHT_PROPERTY);
        if (relationshipProperty.equals(property)) return;

        String tasks = pipeline.tasksByRelationshipProperty(executionContext())
            .get(relationshipProperty)
            .stream()
            .map(s -> "`" + s + "`")
            .collect(Collectors.joining(", "));
        throw new IllegalArgumentException(formatWithLocale(
            "Node property steps added to a pipeline may not have different non-null values for `%s`. " +
            "Pipeline already contains tasks %s which use the value `%s`.",
            RELATIONSHIP_WEIGHT_PROPERTY,
            tasks,
            relationshipProperty
        ));
    }

    private void validateContextRelationshipTypesStar(Map<String, Object> procedureConfig) {
        var contextRelTypes = (List<String>) procedureConfig.get(CONTEXT_RELATIONSHIP_TYPES);
        if (contextRelTypes != null && contextRelTypes.contains(ElementProjection.PROJECT_ALL)) {
            throw new IllegalArgumentException("contextRelationshipTypes cannot be '*' since it will cause test data leakage. Please specify a list of contextRelationshipTypes explicitly.");
        }
    }

}
