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
package org.neo4j.gds.procedures.misc;

import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.algorithms.misc.ScalePropertiesSpecificFields;
import org.neo4j.gds.algorithms.misc.ScaledPropertiesNodePropertyValues;
import org.neo4j.gds.procedures.misc.scaleproperties.ScalePropertiesStatsResult;
import org.neo4j.gds.procedures.misc.scaleproperties.ScalePropertiesStreamResult;
import org.neo4j.gds.procedures.misc.scaleproperties.ScalePropertiesWriteResult;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.scaleproperties.ScalePropertiesStatsConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class ScalePropertiesComputationResultTransformer {

    private ScalePropertiesComputationResultTransformer() {}

    static Stream<ScalePropertiesStreamResult> toStreamResult(
        StreamComputationResult<ScalePropertiesResult> computationResult) {
        return computationResult.result()
                    .map(result -> {
                        var graph = computationResult.graph();
                        var nodeProperties = new ScaledPropertiesNodePropertyValues(graph.nodeCount(), result.scaledProperties());
                        return LongStream
                            .range(0, graph.nodeCount())
                            .mapToObj(nodeId -> new ScalePropertiesStreamResult(
                                graph.toOriginalNodeId(nodeId),
                                nodeProperties.doubleArrayValue(nodeId)
                            ));

        }).orElseGet(Stream::empty);
    }

    static ScalePropertiesStatsResult toStatsResult(
        StatsResult<ScalePropertiesSpecificFields> statsResult,
        boolean returnStatistics,
        ScalePropertiesStatsConfig config
    ) {

        return new ScalePropertiesStatsResult(
            (returnStatistics) ? statsResult.algorithmSpecificFields().scalerStatistics() : null,
            statsResult.preProcessingMillis(),
            statsResult.computeMillis(),
            config.toMap()
        );
    }

    static ScalePropertiesWriteResult toWriteResult(
        NodePropertyWriteResult<ScalePropertiesSpecificFields> writeResult,
        boolean returnStatistics
    ) {

        return new ScalePropertiesWriteResult(
            (returnStatistics) ? writeResult.algorithmSpecificFields().scalerStatistics() : null,
            writeResult.preProcessingMillis(),
            writeResult.computeMillis(),
            writeResult.writeMillis(),
            writeResult.nodePropertiesWritten(),
            writeResult.configuration().toMap()
        );
    }


}
