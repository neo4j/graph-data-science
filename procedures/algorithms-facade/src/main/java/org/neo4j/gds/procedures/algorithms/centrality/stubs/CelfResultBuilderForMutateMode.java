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
package org.neo4j.gds.procedures.algorithms.centrality.stubs;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationMutateConfig;
import org.neo4j.gds.procedures.algorithms.centrality.CELFMutateResult;

import java.util.Optional;

public class CelfResultBuilderForMutateMode implements ResultBuilder<InfluenceMaximizationMutateConfig, CELFResult, CELFMutateResult, NodePropertiesWritten> {
    @Override
    public CELFMutateResult build(
        Graph graph,
        GraphStore graphStore,
        InfluenceMaximizationMutateConfig configuration,
        Optional<CELFResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return CELFMutateResult.emptyFrom(timings, configuration.toMap());

        var celfResult = result.get();

        return CELFMutateResult.builder()
            .withTotalSpread(celfResult.totalSpread())
            .withNodeCount(graph.nodeCount())
            .withPreProcessingMillis(timings.preProcessingMillis)
            .withComputeMillis(timings.computeMillis)
            .withMutateMillis(timings.mutateOrWriteMillis)
            .withNodePropertiesWritten(metadata.orElseThrow().value())
            .withConfig(configuration)
            .build();
    }
}
