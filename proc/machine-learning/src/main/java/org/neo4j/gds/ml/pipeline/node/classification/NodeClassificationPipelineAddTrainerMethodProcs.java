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

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.pipelines.NodePipelineInfoResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class NodeClassificationPipelineAddTrainerMethodProcs {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.beta.pipeline.nodeClassification.addLogisticRegression", mode = READ)
    @Description("Add a logistic regression configuration to the parameter space of the node classification train pipeline.")
    public Stream<NodePipelineInfoResult> addLogisticRegression(
        @Name("pipelineName") String pipelineName,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> logisticRegressionClassifierConfig
    ) {
        return facade.pipelines().addLogisticRegression(pipelineName, logisticRegressionClassifierConfig);
    }

    @Procedure(name = "gds.beta.pipeline.nodeClassification.addRandomForest", mode = READ)
    @Description("Add a random forest configuration to the parameter space of the node classification train pipeline.")
    public Stream<NodePipelineInfoResult> addRandomForest(
        @Name("pipelineName") String pipelineName,
        @Name(value = "config") Map<String, Object> randomForestClassifierConfig
    ) {
        return facade.pipelines().addRandomForest(pipelineName, randomForestClassifierConfig);
    }

    @Procedure(name = "gds.alpha.pipeline.nodeClassification.addRandomForest", mode = READ, deprecatedBy = "gds.beta.pipeline.nodeClassification.addRandomForest")
    @Description("Add a random forest configuration to the parameter space of the node classification train pipeline.")
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<NodePipelineInfoResult> addRandomForestAlpha(
        @Name("pipelineName") String pipelineName,
        @Name(value = "config") Map<String, Object> randomForestClassifierConfig
    ) {
        facade.deprecatedProcedures().called("gds.alpha.pipeline.nodeClassification.addRandomForest");
        facade.log().warn("Procedure `gds.alpha.pipeline.nodeClassification.addRandomForest` has been deprecated, please use `gds.beta.pipeline.nodeClassification.addRandomForest`.");

        return addRandomForest(pipelineName, randomForestClassifierConfig);
    }

    @Procedure(name = "gds.alpha.pipeline.nodeClassification.addMLP", mode = READ)
    @Description("Add a multilayer perceptron configuration to the parameter space of the node classification train pipeline.")
    public Stream<NodePipelineInfoResult> addMLP(
        @Name("pipelineName") String pipelineName,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> mlpClassifierConfig
    ) {
        return facade.pipelines().addMLP(pipelineName, mlpClassifierConfig);
    }
}
