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
import org.neo4j.gds.ml.pipeline.PipelineCompanion;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;
import org.neo4j.gds.procedures.pipelines.NodePipelineInfoResult;
import org.neo4j.gds.procedures.pipelines.NodePipelineInfoResultTransformer;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class NodeRegressionPipelineConfigureAutoTuningProc extends BaseProc {

    @Procedure(name = "gds.alpha.pipeline.nodeRegression.configureAutoTuning", mode = READ)
    @Description("Configures the auto-tuning of a node regression pipeline.")
    public Stream<NodePipelineInfoResult> configureAutoTuning(@Name("pipelineName") String pipelineName, @Name("configuration") Map<String, Object> configMap) {
        PipelineCatalog.getTyped(username(), pipelineName, NodeRegressionTrainingPipeline.class);
        return PipelineCompanion.configureAutoTuning(
            username(),
            pipelineName,
            configMap,
            pipeline -> NodePipelineInfoResultTransformer.create(pipelineName, (NodeRegressionTrainingPipeline) pipeline)
        );
    }

}
