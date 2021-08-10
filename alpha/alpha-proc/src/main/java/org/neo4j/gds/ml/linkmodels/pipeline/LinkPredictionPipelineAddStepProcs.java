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
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.linkmodels.pipeline.PipelineUtils.getPipelineModelInfo;
import static org.neo4j.procedure.Mode.READ;

public class LinkPredictionPipelineAddStepProcs extends BaseProc {

    @Procedure(name = "gds.alpha.ml.pipeline.linkPrediction.addNodeProperty", mode = READ)
    @Description("Add a node property step to an existing link prediction pipeline.")
    public Stream<PipelineInfoResult> addNodeProperty(
        @Name("pipelineName") String pipelineName,
        @Name("procedureName") String taskName,
        @Name("procedureConfiguration") Map<String, Object> procedureConfig
    ) {
        var pipeline = getPipelineModelInfo(pipelineName, username());
        pipeline.addNodePropertyStep(taskName, procedureConfig);

        return Stream.of(new PipelineInfoResult(pipelineName, pipeline));
    }

    @Procedure(name = "gds.alpha.ml.pipeline.linkPrediction.addFeature", mode = READ)
    @Description("Add a feature step to an existing link prediction pipeline.")
    public Stream<PipelineInfoResult> addFeature(
        @Name("pipelineName") String pipelineName,
        @Name("featureName") String featureName,
        @Name("config") Map<String, Object> config
    ) {
        var pipeline = getPipelineModelInfo(pipelineName, username());

        pipeline.addFeatureStep(featureName, config);

        return Stream.of(new PipelineInfoResult(pipelineName, pipeline));
    }
}
