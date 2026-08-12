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
package org.neo4j.gds.applications.algorithms.execution.machinery;

import org.neo4j.gds.api.Graph;

/**
 * At the base, we run the algorithm and record timing.
 *
 * @note this should be _the only_ place in our codebase where we call {@link org.neo4j.gds.Algorithm#compute()},
 *     anything else would be duplication.
 */
class Timer {
    <RESULT> RESULT runAlgorithmAndRecordTiming(
        ComputationTimer computationTimer,
        ConstructAndRun<RESULT> constructAndRun,
        Graph graph
    ) {
        try (var ignored = computationTimer.start()) {
            return constructAndRun.constructAndRun(graph);
        }
    }
}
