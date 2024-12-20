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

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainResult;

import java.util.Optional;
import java.util.stream.Stream;

class NodeRegressionTrainResultRenderer implements ResultRenderer<NodeRegressionTrainResult.NodeRegressionTrainPipelineResult, Stream<NodeRegressionPipelineTrainResult>, Void> {
    @Override
    public Stream<NodeRegressionPipelineTrainResult> render(
        GraphResources unused1,
        Optional<NodeRegressionTrainResult.NodeRegressionTrainPipelineResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Void> unused2
    ) {
        if (result.isEmpty()) return Stream.empty();

        var nodeRegressionTrainPipelineResult = result.get();

        var nodeClassificationPipelineTrainResult = new NodeRegressionPipelineTrainResult(
            nodeRegressionTrainPipelineResult.model(),
            nodeRegressionTrainPipelineResult.trainingStatistics(),
            timings.computeMillis
        );

        return Stream.of(nodeClassificationPipelineTrainResult);
    }
}
