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
package org.neo4j.gds.ml.pipeline.node.classification;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.node.NodePipelineInfoResult;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.pipeline.NodePropertyStepFactory.createNodePropertyStep;
import static org.neo4j.procedure.Mode.READ;

public class NodeClassificationPipelineAddStepProcs extends BaseProc {

    public static NodePipelineInfoResult addNodeProperty(
        String username,
        String pipelineName,
        String taskName,
        Map<String, Object> procedureConfig
    ) {
        return addNodeProperty(username, pipelineName, taskName, procedureConfig, List.of(), List.of());
    }

    public static NodePipelineInfoResult addNodeProperty(
        String username,
        String pipelineName,
        String taskName,
        Map<String, Object> procedureConfig,
        List<String> contextNodeLabels,
        List<String> contextRelationshipTypes
    ) {
        var pipeline = PipelineCatalog.getTyped(username, pipelineName, NodeClassificationTrainingPipeline.class);

        pipeline.addNodePropertyStep(createNodePropertyStep(taskName, procedureConfig));

        return new NodePipelineInfoResult(pipelineName, pipeline);
    }

    public static NodePipelineInfoResult selectFeatures(
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

    @Procedure(name = "gds.beta.pipeline.nodeClassification.addNodeProperty", mode = READ)
    @Description("Add a node property step to an existing node classification training pipeline.")
    public Stream<NodePipelineInfoResult> addNodeProperty(
        @Name("pipelineName") String pipelineName,
        @Name("procedureName") String taskName,
        @Name("procedureConfiguration") Map<String, Object> procedureConfig,
        @Name(value = "contextNodeLabels", defaultValue = "[]") List<String> contextNodeLabels,
        @Name(value = "contextRelationshipTypes", defaultValue = "[]") List<String> contextRelationshipTypes
    ) {
        return Stream.of(addNodeProperty(
            username(),
            pipelineName,
            taskName,
            procedureConfig
        ));
    }

    @Procedure(name = "gds.beta.pipeline.nodeClassification.selectFeatures", mode = READ)
    @Description("Add one or several features to an existing node classification training pipeline.")
    public Stream<NodePipelineInfoResult> selectFeatures(
        @Name("pipelineName") String pipelineName,
        @Name("nodeProperties") Object nodeProperties
    ) {
        return Stream.of(selectFeatures(username(), pipelineName, nodeProperties));
    }
}
