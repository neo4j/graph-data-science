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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.StandardLabel;
import org.neo4j.gds.applications.algorithms.machinery.WriteNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.JobId;

import java.util.Optional;

class NodeClassificationPredictPipelineWriteStep implements WriteStep<NodeClassificationPipelineResult, NodePropertiesWritten> {
    private final WriteNodePropertyService writeNodePropertyService;
    private final NodeClassificationPredictPipelineWriteConfig configuration;

    NodeClassificationPredictPipelineWriteStep(
        WriteNodePropertyService writeNodePropertyService,
        NodeClassificationPredictPipelineWriteConfig configuration
    ) {
        this.writeNodePropertyService = writeNodePropertyService;
        this.configuration = configuration;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        NodeClassificationPipelineResult result,
        JobId jobId
    ) {
        var nodeProperties = PredictedProbabilities.asProperties(
            Optional.of(result),
            configuration.writeProperty(),
            configuration.predictedProbabilityProperty()
        );

        return writeNodePropertyService.perform(
            graph,
            graphStore,
            resultStore,
            configuration,
            new StandardLabel("NodeClassificationPipelineWrite"),
            jobId,
            nodeProperties
        );
    }
}
