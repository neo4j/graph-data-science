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
package org.neo4j.gds.ml.pipeline.node.regression.configure;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.procedures.pipelines.NodePipelineInfoResult;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.pipeline.NodePropertyStepFactory.createNodePropertyStep;
import static org.neo4j.procedure.Mode.READ;

public class NodeRegressionPipelineAddStepProcs extends BaseProc {

    @Procedure(name = "gds.alpha.pipeline.nodeRegression.addNodeProperty", mode = READ)
    @Description("Add a node property step to an existing node regression training pipeline.")
    public Stream<NodePipelineInfoResult> addNodeProperty(
        @Name("pipelineName") String pipelineName,
        @Name("procedureName") String taskName,
        @Name("procedureConfiguration") Map<String, Object> procedureConfig
    ) {
        var pipeline = PipelineCatalog.getTyped(username(), pipelineName, NodeRegressionTrainingPipeline.class);

        pipeline.addNodePropertyStep(createNodePropertyStep(taskName, procedureConfig));

        return Stream.of(NodePipelineInfoResult.create(pipelineName, pipeline));
    }

    @Procedure(name = "gds.alpha.pipeline.nodeRegression.selectFeatures", mode = READ)
    @Description("Add one or several features to an existing node regression training pipeline.")
    public Stream<NodePipelineInfoResult> selectFeatures(
        @Name("pipelineName") String pipelineName,
        @Name("featureProperties") Object featureProperties
    ) {
        var pipeline = PipelineCatalog.getTyped(username(), pipelineName, NodeRegressionTrainingPipeline.class);

        if (featureProperties instanceof String) {
            pipeline.addFeatureStep(NodeFeatureStep.of((String) featureProperties));
        } else if (featureProperties instanceof List) {
            var propertiesList = (List<?>) featureProperties;
            for (Object o : propertiesList) {
                if (!(o instanceof String)) {
                    throw new IllegalArgumentException("The list `featureProperties` is required to contain only strings.");
                }

                pipeline.addFeatureStep(NodeFeatureStep.of((String) o));
            }
        } else {
            throw new IllegalArgumentException("The value of `featureProperties` is required to be a list of strings.");
        }

        return Stream.of(NodePipelineInfoResult.create(pipelineName, pipeline));
    }
}
