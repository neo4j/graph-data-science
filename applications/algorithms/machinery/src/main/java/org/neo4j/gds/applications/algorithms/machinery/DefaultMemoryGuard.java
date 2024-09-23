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
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryGauge;
import org.neo4j.gds.utils.StringFormatting;

import java.util.function.Supplier;

public class DefaultMemoryGuard implements MemoryGuard {
    private final Log log;
    private final boolean useMaxMemoryEstimation;
    private final MemoryGauge memoryGauge;

    public DefaultMemoryGuard(Log log, boolean useMaxMemoryEstimation, MemoryGauge memoryGauge) {
        this.log = log;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.memoryGauge = memoryGauge;
    }

    @Override
    public <CONFIGURATION extends ConcurrencyConfig> void assertAlgorithmCanRun(
        Label label,
        CONFIGURATION configuration,
        Graph graph,
        Supplier<MemoryEstimation> estimationFactory
    ) throws IllegalStateException {
        try {
            var memoryEstimation = estimationFactory.get();

            var graphDimensions = GraphDimensions.of(graph.nodeCount(), graph.relationshipCount());

            var memoryTree = memoryEstimation.estimate(graphDimensions, configuration.concurrency());

            var memoryRange = memoryTree.memoryUsage();

            var bytesRequired = useMaxMemoryEstimation ? memoryRange.max : memoryRange.min;

            assertAlgorithmCanRun(label, bytesRequired);
        } catch (MemoryEstimationNotImplementedException e) {
            log.info("Memory usage estimate not available for " + label + ", skipping guard");
        }
    }

    private void assertAlgorithmCanRun(Label label, long bytesRequired)
    throws IllegalStateException {
        long bytesAvailable = memoryGauge.availableMemory();

        if (bytesRequired > bytesAvailable) {
            var message = StringFormatting.formatWithLocale(
                "Memory required to run %s (%db) exceeds available memory (%db)",
                label,
                bytesRequired,
                bytesAvailable
            );

            throw new IllegalStateException(message);
        }
    }
}
