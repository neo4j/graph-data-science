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
package org.neo4j.gds.procedures.algorithms.community.stubs;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.procedures.algorithms.community.KCoreDecompositionMutateResult;

import java.util.Optional;

public class KCoreResultBuilderForMutateMode implements ResultBuilder<KCoreDecompositionMutateConfig, KCoreDecompositionResult, KCoreDecompositionMutateResult, NodePropertiesWritten> {
    @Override
    public KCoreDecompositionMutateResult build(
        Graph graph,
        KCoreDecompositionMutateConfig configuration,
        Optional<KCoreDecompositionResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return KCoreDecompositionMutateResult.emptyFrom(timings, configuration.toMap());

        var kCoreDecompositionResult = result.get();

        return new KCoreDecompositionMutateResult(
            metadata.orElseThrow().value(),
            kCoreDecompositionResult.degeneracy(),
            timings.preProcessingMillis,
            timings.computeMillis,
            0,
            timings.mutateOrWriteMillis,
            configuration.toMap()
        );
    }
}
