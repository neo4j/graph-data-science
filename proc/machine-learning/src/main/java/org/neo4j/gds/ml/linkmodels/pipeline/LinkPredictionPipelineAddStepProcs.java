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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStepFactory;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.LinkFeatureStepConfigurationImpl;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.pipelines.PipelineInfoResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LinkPredictionPipelineAddStepProcs extends BaseProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.beta.pipeline.linkPrediction.addNodeProperty", mode = READ)
    @Description("Add a node property step to an existing link prediction pipeline.")
    public Stream<PipelineInfoResult> addNodeProperty(
        @Name("pipelineName") String pipelineName,
        @Name("procedureName") String taskName,
        @Name("procedureConfiguration") Map<String, Object> procedureConfig
    ) {
        return facade.pipelines().linkPrediction().addNodeProperty(pipelineName, taskName, procedureConfig);
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

        return Stream.of(PipelineInfoResult.create(pipelineName, pipeline));
    }
}
