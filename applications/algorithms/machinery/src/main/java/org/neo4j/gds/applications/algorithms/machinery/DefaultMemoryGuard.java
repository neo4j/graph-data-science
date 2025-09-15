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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.services.GraphDimensionFactory;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryReservationExceededException;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.utils.StringFormatting;

import java.util.Collection;
import java.util.function.Supplier;

public final class DefaultMemoryGuard implements MemoryGuard {
    private final Log log;
    private final GraphDimensionFactory graphDimensionFactory;
    private final boolean useMaxMemoryEstimation;
    private final MemoryTracker memoryTracker;

    DefaultMemoryGuard(
        Log log,
        GraphDimensionFactory graphDimensionFactory,
        boolean useMaxMemoryEstimation,
        MemoryTracker memoryTracker
    ) {
        this.log = log;
        this.graphDimensionFactory = graphDimensionFactory;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.memoryTracker = memoryTracker;
    }

    public static DefaultMemoryGuard create(
        Log log,
        boolean useMaxMemoryEstimation,
        MemoryTracker memoryTracker
    ) {
        var graphDimensionFactory = new GraphDimensionFactory();

        return new DefaultMemoryGuard(log, graphDimensionFactory, useMaxMemoryEstimation, memoryTracker);
    }

    @Override
    public synchronized void assertAlgorithmCanRun(
        Graph graph,
        GraphStore graphStore,
        Collection<RelationshipType> relationshipTypesFilter,
        Concurrency concurrency,
        Supplier<MemoryEstimation> estimationFactory,
        Label label,
        DimensionTransformer dimensionTransformer,
        String username,
        JobId jobId,
        boolean bypassMemoryEstimation
    ) throws IllegalStateException {

        try {
            var memoryRequirement = MemoryRequirement.create(
                estimationFactory.get(),
                graphDimensionFactory.create(graph, graphStore, relationshipTypesFilter),
                dimensionTransformer,
                concurrency,
                useMaxMemoryEstimation
            );

            var bytesToReserve = memoryRequirement.requiredMemory();
            if (bypassMemoryEstimation) {
                memoryTracker.track(username,label.asString(), jobId, bytesToReserve);
                return;
            }

            memoryTracker.tryToTrack(username, label.asString(), jobId, bytesToReserve);

        } catch (MemoryEstimationNotImplementedException e) {
            log.info("Memory usage estimate not available for " + label + ", skipping guard");
        } catch (MemoryReservationExceededException e) {
            var message = StringFormatting.formatWithLocale(
                "Memory required to run %s (%db) exceeds available memory (%db)",
                label,
                e.bytesRequired(),
                e.bytesAvailable()
            );

            throw new IllegalStateException(message);

        }
    }

}
