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
package org.neo4j.gds.mem;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.procedures.LongProcedure;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEvent;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEventListener;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreRemovedEvent;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreRemovedEventListener;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskStoreListener;
import org.neo4j.gds.core.utils.progress.UserTask;
import org.neo4j.gds.logging.Log;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryTracker implements TaskStoreListener, GraphStoreAddedEventListener, GraphStoreRemovedEventListener {
    private final long initialMemory;

    private final AtomicLong graphStoresMemory = new AtomicLong();

    private final ObjectLongMap<JobId> memoryInUse = new ObjectLongHashMap<>();
    private final Log log;

    public MemoryTracker(long initialMemory, Log log) {
        this.log = log;
        assertPositiveInitialMemory(initialMemory);
        this.initialMemory = initialMemory;
    }

    public long initialMemory() {
        return initialMemory;
    }

    public synchronized void track(JobId jobId, long memoryEstimate) {
        log.debug("Tracking %s:  %s bytes", jobId.asString(), memoryEstimate);
        memoryInUse.put(jobId, memoryEstimate);
        log.debug("Available memory after tracking task: %s bytes", availableMemory());
    }

    public synchronized void tryToTrack(JobId jobId, long memoryEstimate) throws MemoryReservationExceededException {
        var availableMemory = availableMemory();
        if (memoryEstimate > availableMemory) {
            throw new MemoryReservationExceededException(memoryEstimate, availableMemory);
        }
        track(jobId, memoryEstimate);
    }

    public synchronized long availableMemory() {
        var reservedMemory = new LongAdder();
        memoryInUse.values().forEach((LongProcedure) reservedMemory::add);
        return initialMemory - (reservedMemory.longValue() + graphStoresMemory.longValue());
    }

    @Override
    public void onTaskAdded(UserTask userTask) {
        // do nothing, we add the memory explicitly prior to execution
    }

    @Override
    public synchronized void onTaskRemoved(UserTask userTask) {
        var taskDescription = userTask.task().description();
        log.debug("Removing task: %s", taskDescription);
        var jobId = userTask.jobId();
        var removed = memoryInUse.remove(jobId);
        log.debug("Removed task %s (%s):  %s bytes", taskDescription, jobId.asString(), removed);
        log.debug("Available memory after removing task: %s bytes", availableMemory());
        log.debug("Done removing task: %s", taskDescription);
    }

    private static void assertPositiveInitialMemory(long initialMemory) {
        if (initialMemory < 0) {
            throw new IllegalArgumentException(
                formatWithLocale("Negative values are not allowed. Trying to use: `%s`", initialMemory)
            );
        }
    }

    @Override
    public void onGraphStoreAdded(GraphStoreAddedEvent graphStoreAddedEvent) {
        var addedGraphMemory = graphStoreAddedEvent.memoryInBytes();
        var graphsMemory = graphStoresMemory.addAndGet(addedGraphMemory);
        log.debug(
            "Added graph %s, which added another %s bytes, now there are %s bytes occupied by projected graphs",
            graphStoreAddedEvent.graphName(),
            addedGraphMemory,
            graphsMemory
        );
    }

    @Override
    public void onGraphStoreRemoved(GraphStoreRemovedEvent graphStoreAddedEvent) {
        var graphMemoryToRemove = graphStoreAddedEvent.memoryInBytes();
        var graphsMemoryAfterRemoval = graphStoresMemory.addAndGet(-graphMemoryToRemove);
        log.debug(
            "Removed graph %s, which freed %s bytes, there are still %s bytes occupied by projected graphs",
            graphStoreAddedEvent.graphName(),
            graphMemoryToRemove,
            graphsMemoryAfterRemoval
        );
    }
}
