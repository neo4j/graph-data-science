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

import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.procedures.community.modularity.ModularityStreamResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class ModularityComputationResultTransformer {

    private ModularityComputationResultTransformer() {}

    static Stream<ModularityStreamResult> toStreamResult(StreamComputationResult<ModularityResult> computationResult) {
        return computationResult.result().map(modularityResult -> {
            var communityModularities = modularityResult.modularityScores();
            return LongStream
                .range(0, modularityResult.communityCount())
                .mapToObj(communityModularities::get)
                .map(ModularityStreamResult::from);

        }).orElseGet(Stream::empty);
    }


}
