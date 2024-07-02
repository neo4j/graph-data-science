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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.values.storable.Values;

import java.util.Optional;
import java.util.function.LongUnaryOperator;

public abstract class RelationshipExporterBuilder {

    public static final Concurrency TYPED_DEFAULT_WRITE_CONCURRENCY = new Concurrency(1);

    protected LongUnaryOperator toOriginalId;
    protected TerminationFlag terminationFlag;
    protected Graph graph;
    protected ProgressTracker progressTracker = ProgressTracker.NULL_TRACKER;
    protected RelationshipPropertyTranslator propertyTranslator = Values::doubleValue;
    protected long batchSize = NativeNodePropertyExporter.MIN_BATCH_SIZE;
    protected Optional<String> remoteDatabaseName; // coupled with arrowConnectionInfo, but should not appear in external API
    protected Optional<ResultStore> resultStore;
    protected JobId jobId;

    public abstract RelationshipExporter build();

    public RelationshipExporterBuilder withRelationPropertyTranslator(RelationshipPropertyTranslator propertyTranslator) {
        this.propertyTranslator = propertyTranslator;
        return this;
    }

    public RelationshipExporterBuilder withGraph(Graph graph) {
        this.graph = graph;
        return this;
    }

    public RelationshipExporterBuilder withIdMappingOperator(LongUnaryOperator toOriginalId) {
        this.toOriginalId = toOriginalId;
        return this;
    }

    public RelationshipExporterBuilder withTerminationFlag(TerminationFlag terminationFlag) {
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
    public RelationshipExporterBuilder withProgressTracker(ProgressTracker progressTracker) {
        this.progressTracker = progressTracker;
        return this;
    }

    public RelationshipExporterBuilder withBatchSize(long batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public RelationshipExporterBuilder withResultStore(Optional<ResultStore> resultStore) {
        this.resultStore = resultStore;
        return this;
    }

    public RelationshipExporterBuilder withJobId(JobId jobId){
        this.jobId = jobId;
        return this;
    }
}
