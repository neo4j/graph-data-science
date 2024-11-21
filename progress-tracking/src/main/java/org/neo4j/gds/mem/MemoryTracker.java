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

import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEvent;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEventListener;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreRemovedEvent;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreRemovedEventListener;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskStoreListener;
import org.neo4j.gds.core.utils.progress.UserTask;
import org.neo4j.gds.logging.Log;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryTracker implements TaskStoreListener, GraphStoreAddedEventListener, GraphStoreRemovedEventListener {
    private final long initialMemory;
    private final  GraphStoreMemoryContainer  graphStoreMemoryContainer = new GraphStoreMemoryContainer();
    private final  TaskMemoryContainer taskMemoryContainer = new TaskMemoryContainer();
    private final  Log log;

    public MemoryTracker(long initialMemory, Log log) {
        this.log = log;
        assertPositiveInitialMemory(initialMemory);
        this.initialMemory = initialMemory;
    }

    public long initialMemory() {
        return initialMemory;
    }

    public synchronized void track(String username, String taskName, JobId jobId, long memoryEstimate) {
        log.debug("Tracking %s:  %s bytes", jobId.asString(), memoryEstimate);
        taskMemoryContainer.reserve(username,taskName,jobId, memoryEstimate);
        log.debug("Available memory after tracking task: %s bytes", availableMemory());
    }

    public synchronized void tryToTrack(String username, String taskName,JobId jobId, long memoryEstimate) throws MemoryReservationExceededException {
        var availableMemory = availableMemory();
        if (memoryEstimate > availableMemory) {
            throw new MemoryReservationExceededException(memoryEstimate, availableMemory);
        }
        track(username,taskName,jobId, memoryEstimate);
    }

    public synchronized long availableMemory() {
        return initialMemory - graphStoreMemoryContainer.graphStoreReservedMemory() - taskMemoryContainer.taskReservedMemory();
    }

    public Stream<UserEntityMemory> listUser(String user){
        return  Stream.concat(taskMemoryContainer.listTasks(user), graphStoreMemoryContainer.listGraphs(user));
    }

    public Stream<UserEntityMemory> listAll(){
        return  Stream.concat(taskMemoryContainer.listTasks(), graphStoreMemoryContainer.listGraphs());
    }

    public UserMemorySummary memorySummary(String user){
        return  new UserMemorySummary(user,
            graphStoreMemoryContainer.memoryOfGraphs(user),
            taskMemoryContainer.memoryOfTasks(user)
        );
    }

    public Stream<UserMemorySummary> memorySummary(){

        var  users = graphStoreMemoryContainer.graphUsers(Optional.empty());
        users= taskMemoryContainer.taskUsers(Optional.of(users));

        return  users.stream()
            .map(user -> new UserMemorySummary(
                user,
                graphStoreMemoryContainer.memoryOfGraphs(user),
                taskMemoryContainer.memoryOfTasks(user)
            ));
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
        var removed= taskMemoryContainer.removeTask(userTask);
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
        var graphsMemory = graphStoreMemoryContainer.addGraph(graphStoreAddedEvent);
        log.debug(
            "Added graph %s, which added another %s bytes, now there are %s bytes occupied by projected graphs",
            graphStoreAddedEvent.graphName(),
            graphStoreAddedEvent.memoryInBytes(),
            graphsMemory
        );
    }

    @Override
    public void onGraphStoreRemoved(GraphStoreRemovedEvent graphStoreRemovedEvent) {

       var graphsMemoryAfterRemoval= graphStoreMemoryContainer.removeGraph(graphStoreRemovedEvent);
        log.debug(
            "Removed graph %s, which freed %s bytes, there are still %s bytes occupied by projected graphs",
            graphStoreRemovedEvent.graphName(),
            graphStoreRemovedEvent.memoryInBytes(),
            graphsMemoryAfterRemoval
        );
    }
}
