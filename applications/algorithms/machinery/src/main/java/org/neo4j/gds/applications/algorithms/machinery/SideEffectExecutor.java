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
package org.neo4j.gds.applications.algorithms.machinery;

import java.util.Optional;
import java.util.function.Function;

class SideEffectExecutor {
    /**
     * Reusable boilerplate
     */
    <RESULT_FROM_ALGORITHM, METADATA> Optional<METADATA> executeSideEffect(
        Optional<RESULT_FROM_ALGORITHM> result,
        Function<RESULT_FROM_ALGORITHM, METADATA> sideEffect
    ) {
        if (result.isEmpty()) return Optional.empty();

        var resultFromAlgorithm = result.get();

        var metadata = sideEffect.apply(resultFromAlgorithm);

        return Optional.ofNullable(metadata);
    }
}
