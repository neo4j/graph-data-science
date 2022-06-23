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
package org.neo4j.gds.core.io.db;

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.compat.CompatExecutionMonitor;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.internal.batchimport.CountGroupsStage;
import org.neo4j.internal.batchimport.DataImporter;
import org.neo4j.internal.batchimport.NodeCountsAndLabelIndexBuildStage;
import org.neo4j.internal.batchimport.NodeDegreeCountStage;
import org.neo4j.internal.batchimport.NodeFirstGroupStage;
import org.neo4j.internal.batchimport.RelationshipCountsAndTypeIndexBuildStage;
import org.neo4j.internal.batchimport.RelationshipGroupStage;
import org.neo4j.internal.batchimport.RelationshipLinkbackStage;
import org.neo4j.internal.batchimport.RelationshipLinkforwardStage;
import org.neo4j.internal.batchimport.ScanAndCacheGroupsStage;
import org.neo4j.internal.batchimport.SparseNodeFirstRelationshipStage;
import org.neo4j.internal.batchimport.WriteGroupsStage;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.StageExecution;
import org.neo4j.internal.batchimport.stats.Keys;
import org.neo4j.internal.batchimport.stats.Stat;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.helpers.collection.Iterables;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

public class ProgressTrackerExecutionMonitor implements CompatExecutionMonitor {

    private final Clock clock;
    private final long intervalMillis;

    // The total task volume per stage.
    // Set to 0 at the beginning of a stage.
    private final AtomicLong stageProgressTotal;
    // The progressed task volume per stage.
    // Set to 0 at the beginning of a stage.
    private final AtomicLong stageProgressCurrent;

    private final ProgressTracker progressTracker;

    private DependencyResolver dependencyResolver;

    public static Task progressTask(long nodeCount, long relationshipCount) {
        return Tasks.task(
            GraphStoreToDatabaseExporter.class.getSimpleName(),
            Tasks.leaf(DataImporter.NODE_IMPORT_NAME, nodeCount),
            Tasks.leaf(DataImporter.RELATIONSHIP_IMPORT_NAME, relationshipCount),
            Tasks.leaf(NodeDegreeCountStage.NAME),
            Tasks.leaf(RelationshipLinkforwardStage.NAME, 2 * relationshipCount),
            Tasks.leaf(RelationshipGroupStage.NAME),
            Tasks.leaf(SparseNodeFirstRelationshipStage.NAME),
            Tasks.leaf(RelationshipLinkbackStage.NAME, 2 * relationshipCount),
            Tasks.leaf(CountGroupsStage.NAME),
            Tasks.leaf(ScanAndCacheGroupsStage.NAME),
            Tasks.leaf(WriteGroupsStage.NAME),
            Tasks.leaf(NodeFirstGroupStage.NAME),
            Tasks.leaf(NodeCountsAndLabelIndexBuildStage.NAME, nodeCount),
            Tasks.leaf(RelationshipCountsAndTypeIndexBuildStage.NAME, relationshipCount)
        );
    }

    public static ExecutionMonitor of(
        ProgressTracker progressTracker,
        Clock clock,
        long time,
        TimeUnit unit
    ) {
        return Neo4jProxy.executionMonitor(new ProgressTrackerExecutionMonitor(progressTracker, clock, time, unit));
    }

    private ProgressTrackerExecutionMonitor(ProgressTracker progressTracker, Clock clock, long time, TimeUnit unit) {
        this.clock = clock;
        this.intervalMillis = unit.toMillis(time);
        this.progressTracker = progressTracker;
        this.stageProgressTotal = new AtomicLong(0);
        this.stageProgressCurrent = new AtomicLong(0);
    }

    @Override
    public void initialize(DependencyResolver dependencyResolver) {
        this.dependencyResolver = dependencyResolver;
        this.progressTracker.beginSubTask();
    }

    @Override
    public void start(StageExecution execution) {
        this.stageProgressTotal.set(0);
        this.stageProgressCurrent.set(0);

        var neoStores = this.dependencyResolver.resolveDependency(BatchingNeoStores.class);
        var relationshipRecordIdCount = neoStores.getRelationshipStore().getHighId();
        var groupCount = neoStores.getTemporaryRelationshipGroupStore().getHighId();

        switch (execution.getStageName()) {
            case NodeDegreeCountStage.NAME:
                this.progressTracker.beginSubTask(execution.getStageName(), relationshipRecordIdCount);
                break;
            case CountGroupsStage.NAME:
            case WriteGroupsStage.NAME:
            case NodeFirstGroupStage.NAME:
                this.progressTracker.beginSubTask(execution.getStageName(), groupCount);
                break;
            default:
                this.progressTracker.beginSubTask(execution.getStageName());
        }

        if (this.progressTracker.currentVolume() != Task.UNKNOWN_VOLUME) {
            this.stageProgressTotal.set(this.progressTracker.currentVolume());
        }
    }

    @Override
    public void end(StageExecution execution, long totalTimeMillis) {
        this.progressTracker.endSubTask(execution.getStageName());
    }

    @Override
    public void done(boolean successful, long totalTimeMillis, String additionalInformation) {
        this.progressTracker.endSubTask();
        this.progressTracker.logInfo(additionalInformation);
    }

    @Override
    public void check(StageExecution execution) {
        // Cap the total to not produce percentages > 100.
        var progress = Math.min(progress(execution), this.stageProgressTotal.get());
        this.progressTracker.logProgress(progress - this.stageProgressCurrent.getAndSet(progress));
    }

    private static long progress(StageExecution execution) {
        return StreamSupport
            .stream(execution.steps().spliterator(), false)
            .map(step -> step.stats().stat(Keys.progress))
            .filter(Objects::nonNull)
            .map(Stat::asLong)
            .findFirst()
            .orElseGet(() -> {
                var doneBatches = Iterables.last(execution.steps()).stats().stat(Keys.done_batches).asLong();
                var batchSize = execution.getConfig().batchSize();
                return doneBatches * batchSize;
            });
    }

    @Override
    public Clock clock() {
        return this.clock;
    }

    @Override
    public long checkIntervalMillis() {
        return this.intervalMillis;
    }
}
