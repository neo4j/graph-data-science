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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.mem.MemoryEstimation;

import java.util.function.Supplier;

/**
 * This is just memory guarding. Do not conflate with UI concerns.
 */
public interface MemoryGuard {
    /**
     * This could be handy for tests
     */
    MemoryGuard DISABLED = new MemoryGuard() {
        @Override
        public <CONFIGURATION extends ConcurrencyConfig> void assertAlgorithmCanRun(
            Label label,
            CONFIGURATION configuration,
            Graph graph,
            Supplier<MemoryEstimation> estimationFactory
        ) {
            // do nothing
        }
    };

    /**
     * Measure how much memory is needed vs how much is available.
     *
     * @throws IllegalStateException when there is not enough memory available to run the algorithm in the given configuration on the given graph
     */
    <CONFIGURATION extends ConcurrencyConfig> void assertAlgorithmCanRun(
        Label label,
        CONFIGURATION configuration,
        Graph graph,
        Supplier<MemoryEstimation> estimationFactory
    ) throws IllegalStateException;
}
