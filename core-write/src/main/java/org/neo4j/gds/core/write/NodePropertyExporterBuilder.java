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
package org.neo4j.gds.core.write;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;

public abstract class NodePropertyExporterBuilder {
    protected long nodeCount;
    protected Set<NodeLabel> nodeLabels;
    protected LongUnaryOperator toOriginalId;
    protected TerminationFlag terminationFlag;

    protected ExecutorService executorService;
    protected Concurrency writeConcurrency = ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY;
    protected ProgressTracker progressTracker = ProgressTracker.NULL_TRACKER;
    protected Optional<String> remoteDatabaseName; // coupled with arrowConnectionInfo, but should not appear in external API
    protected Optional<ResultStore> resultStore = Optional.empty();
    protected JobId jobId;

    public abstract NodePropertyExporter build();

    public NodePropertyExporterBuilder withIdMap(IdMap idMap) {
        Objects.requireNonNull(idMap);
        this.nodeCount = idMap.nodeCount();
        this.nodeLabels = idMap.availableNodeLabels();
        this.toOriginalId = idMap::toOriginalNodeId;
        return this;
    }

    public NodePropertyExporterBuilder withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    /**
     * Set the {@link ProgressTracker} to use for logging progress during export.
     *
     * If a {@link org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker} is used, caller must manage beginning and finishing the subtasks.
     * By default, an {@link org.neo4j.gds.core.utils.progress.tasks.ProgressTracker.EmptyProgressTracker} is used. That one doesn't require caller to manage any tasks.
     *
     * @param progressTracker The progress tracker to use for logging progress during export.
     * @return this
     */
    public NodePropertyExporterBuilder withProgressTracker(ProgressTracker progressTracker) {
        this.progressTracker = progressTracker;
        return this;
    }

    public NodePropertyExporterBuilder parallel(ExecutorService es, Concurrency writeConcurrency) {
        this.executorService = es;
        this.writeConcurrency = writeConcurrency;
        return this;
    }

    public NodePropertyExporterBuilder withResultStore(Optional<ResultStore> resultStore) {
        this.resultStore = resultStore;
        return this;
    }

    public NodePropertyExporterBuilder withJobId(JobId jobId){
        this.jobId = jobId;
        return this;
    }
}
