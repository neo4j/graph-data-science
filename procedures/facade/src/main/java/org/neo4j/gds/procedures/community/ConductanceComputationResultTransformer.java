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
package org.neo4j.gds.procedures.community;

import org.neo4j.gds.algorithms.community.specificfields.K1ColoringSpecificFields;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.procedures.community.conductance.ConductanceStreamResult;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringMutateResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class ConductanceComputationResultTransformer {

    private ConductanceComputationResultTransformer() {}

    static Stream<ConductanceStreamResult> toStreamResult(
        StreamComputationResult<ConductanceResult> computationResult) {
        return computationResult.result().map(conductanceResult -> {
            var condunctances = conductanceResult.communityConductances();
            return LongStream
                .range(0, condunctances.capacity())
                .filter(community -> !Double.isNaN(condunctances.get(community)))
                .mapToObj(community -> new ConductanceStreamResult(community, condunctances.get(community)));

        }).orElseGet(Stream::empty);
    }

    static K1ColoringMutateResult toMutateResult(NodePropertyMutateResult<K1ColoringSpecificFields> computationResult) {
        return new K1ColoringMutateResult(
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.mutateMillis(),
            computationResult.algorithmSpecificFields().nodeCount(),
            computationResult.algorithmSpecificFields().colorCount(),
            computationResult.algorithmSpecificFields().ranIterations(),
            computationResult.algorithmSpecificFields().didConverge(),
            computationResult.configuration().toMap()
        );
    }


}
