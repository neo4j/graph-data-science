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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.services.GraphDimensionFactory;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryGauge;
import org.neo4j.gds.mem.MemoryReservationExceededException;
import org.neo4j.gds.utils.StringFormatting;

import java.util.function.Supplier;

public final class DefaultMemoryGuard implements MemoryGuard {
    private final Log log;
    private final GraphDimensionFactory graphDimensionFactory;
    private final boolean useMaxMemoryEstimation;
    private final MemoryGauge memoryGauge;

    DefaultMemoryGuard(
        Log log,
        GraphDimensionFactory graphDimensionFactory,
        boolean useMaxMemoryEstimation,
        MemoryGauge memoryGauge
    ) {
        this.log = log;
        this.graphDimensionFactory = graphDimensionFactory;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.memoryGauge = memoryGauge;
    }

    public static DefaultMemoryGuard create(Log log, boolean useMaxMemoryEstimation, MemoryGauge memoryGauge) {
        var graphDimensionFactory = new GraphDimensionFactory();

        return new DefaultMemoryGuard(log, graphDimensionFactory, useMaxMemoryEstimation, memoryGauge);
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig> void assertAlgorithmCanRun(
        Supplier<MemoryEstimation> estimationFactory,
        GraphStore graphStore,
        CONFIGURATION configuration,
        Label label,
        DimensionTransformer dimensionTransformer
    ) throws IllegalStateException {
        if (configuration.sudo()) return;

        try {
            var memoryEstimation = estimationFactory.get();

            var graphDimensions = graphDimensionFactory.create(graphStore, configuration);

            var transformedGraphDimensions = dimensionTransformer.transform(graphDimensions);

            var memoryTree = memoryEstimation.estimate(transformedGraphDimensions, configuration.concurrency());

            var memoryRange = memoryTree.memoryUsage();

            var bytesRequired = useMaxMemoryEstimation ? memoryRange.max : memoryRange.min;

            try {
                memoryGauge.tryToReserveMemory(bytesRequired);
            } catch (MemoryReservationExceededException e) {
                var message = StringFormatting.formatWithLocale(
                    "Memory required to run %s (%db) exceeds available memory (%db)",
                    label,
                    e.bytesRequired(),
                    e.bytesAvailable()
                );

                throw new IllegalStateException(message);

            }
        } catch (MemoryEstimationNotImplementedException e) {
            log.info("Memory usage estimate not available for " + label + ", skipping guard");
        }
    }
}
