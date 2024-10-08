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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationWriteConfig;

import java.util.Optional;
import java.util.stream.Stream;

class CelfResultBuilderForWriteMode implements ResultBuilder<InfluenceMaximizationWriteConfig, CELFResult, Stream<CELFWriteResult>, NodePropertiesWritten> {
    @Override
    public Stream<CELFWriteResult> build(
        Graph graph,
        InfluenceMaximizationWriteConfig configuration,
        Optional<CELFResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> nodePropertiesWritten
    ) {
        if (result.isEmpty()) return Stream.of(CELFWriteResult.emptyFrom(
            timings,
            configuration.toMap()
        ));

        var celfResult = result.get();

        return Stream.of(
            new CELFWriteResult(
                timings.sideEffectMillis,
                nodePropertiesWritten.orElseThrow().value(),
                timings.computeMillis,
                celfResult.totalSpread(),
                graph.nodeCount(),
                configuration.toMap()
            )
        );
    }
}
