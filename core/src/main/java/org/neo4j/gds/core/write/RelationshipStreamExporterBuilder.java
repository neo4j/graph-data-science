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

import org.neo4j.gds.api.IdMapping;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.TransactionContext;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.logging.Log;

import java.util.Objects;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

public abstract class RelationshipStreamExporterBuilder<T extends RelationshipStreamExporter> {
    protected final TransactionContext transactionContext;
    protected Stream<Relationship> relationships;
    protected int batchSize;
    protected LongUnaryOperator toOriginalId;
    protected TerminationFlag terminationFlag;
    protected ProgressLogger progressLogger;

    protected ProgressTracker progressTracker;

    RelationshipStreamExporterBuilder(TransactionContext transactionContext) {
        this.transactionContext = Objects.requireNonNull(transactionContext);
        this.progressLogger = ProgressLogger.NULL_LOGGER;
        this.batchSize = (int) NativeNodePropertyExporter.MIN_BATCH_SIZE;
    }

    public final T build() {
        prepareProgressTracker();

        return actuallyBuild();
    }

    private void prepareProgressTracker() {
        Task task = Tasks.leaf(taskName());
        progressTracker = new TaskProgressTracker(task, progressLogger);
    }

    protected abstract T actuallyBuild();

    public RelationshipStreamExporterBuilder<T> withIdMapping(IdMapping idMapping) {
        Objects.requireNonNull(idMapping);
        this.toOriginalId = idMapping::toOriginalNodeId;
        return this;
    }

    public RelationshipStreamExporterBuilder<T> withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    public RelationshipStreamExporterBuilder<T> withLog(Log log) {
        return withProgressLogger(new BatchingProgressLogger(log, Tasks.leaf(taskName(), 0), ConcurrencyConfig.DEFAULT_CONCURRENCY));
    }

    public RelationshipStreamExporterBuilder<T> withRelationships(Stream<Relationship> relationships) {
        this.relationships = relationships;
        return this;
    }

    RelationshipStreamExporterBuilder<T> withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    private String taskName() {
        return "WriteRelationshipStream";
    }

    private RelationshipStreamExporterBuilder<T> withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }
}
