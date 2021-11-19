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
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.neo4j.gds.BaseProc;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCompanion.getNCPipeline;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepFactory.createNodePropertyStep;

public class NodeClassificationPipelineAddSteps {

    public static PipelineInfoResult addNodeProperty(
        String username,
        BaseProc caller,
        String pipelineName,
        String taskName,
        Map<String, Object> procedureConfig
    ) {
        var pipeline = getNCPipeline(pipelineName, username);

        pipeline.addNodePropertyStep(createNodePropertyStep(caller, taskName, procedureConfig));

        return new PipelineInfoResult(pipelineName, pipeline);
    }

    public static PipelineInfoResult addFeatures(
        String username,
        String pipelineName,
        Object nodeProperties
    ) {
        var pipeline = getNCPipeline(pipelineName, username);

        if (nodeProperties instanceof String) {
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of((String) nodeProperties));
        } else if (nodeProperties instanceof List) {
            var propertiesList = (List) nodeProperties;
            for (Object o : propertiesList) {
                if (!(o instanceof String)) {
                    throw new IllegalArgumentException("The list `nodeProperties` is required to contain only strings.");
                }

                pipeline.addFeatureStep(NodeClassificationFeatureStep.of((String) o));
            }
        } else {
            throw new IllegalArgumentException("The value of `nodeProperties` is required to be a list of strings.");
        }

        return new PipelineInfoResult(pipelineName, pipeline);
    }
}
