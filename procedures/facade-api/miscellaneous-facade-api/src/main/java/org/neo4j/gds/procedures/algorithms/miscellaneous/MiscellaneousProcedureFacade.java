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
package org.neo4j.gds.procedures.algorithms.miscellaneous;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.CollapsePathMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.IndexInverseMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.ScalePropertiesMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.ToUndirectedMutateStub;

import java.util.Map;
import java.util.stream.Stream;

public interface MiscellaneousProcedureFacade {
    ScalePropertiesMutateStub alphaScalePropertiesMutateStub();

    Stream<ScalePropertiesStreamResult> alphaScalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
    );

    CollapsePathMutateStub collapsePathMutateStub();

    IndexInverseMutateStub indexInverseMutateStub();

    ScalePropertiesMutateStub scalePropertiesMutateStub();

    Stream<ScalePropertiesStatsResult> scalePropertiesStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> scalePropertiesStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ScalePropertiesStreamResult> scalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> scalePropertiesStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ScalePropertiesWriteResult> scalePropertiesWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> scalePropertiesWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    ToUndirectedMutateStub toUndirectedMutateStub();
}
