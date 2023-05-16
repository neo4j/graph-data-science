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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.config.WriteConfig;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

public abstract class RelationshipStreamExporterBuilder {
    protected Stream<ExportedRelationship> relationships;

    // FIXME: This looks very dodgy; only being overriden in the Cypher implementation and in some tests...
    protected int batchSize = (int) NativeNodePropertyExporter.MIN_BATCH_SIZE;
    protected LongUnaryOperator toOriginalId;
    protected TerminationFlag terminationFlag;
    protected ProgressTracker progressTracker = ProgressTracker.NULL_TRACKER;
    protected long relationshipCount = -1L;
    protected Optional<WriteConfig.ArrowConnectionInfo> arrowConnectionInfo;
    protected String databaseName; // coupled with arrowConnectionInfo, but should not appear in external API

    public abstract RelationshipStreamExporter build();

    public RelationshipStreamExporterBuilder withIdMappingOperator(LongUnaryOperator toOriginalNodeId) {
        this.toOriginalId = toOriginalNodeId;
        return this;
    }

    public RelationshipStreamExporterBuilder withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    public RelationshipStreamExporterBuilder withRelationships(Stream<ExportedRelationship> relationships) {
        this.relationships = relationships;
        return this;
    }

    public RelationshipStreamExporterBuilder withRelationshipCount(long relationshipCount) {
        this.relationshipCount = relationshipCount;
        return this;
    }

    @TestOnly
    public RelationshipStreamExporterBuilder withBatchSize(int batchSize) {
        this.batchSize = batchSize;
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
    public RelationshipStreamExporterBuilder withProgressTracker(ProgressTracker progressTracker) {
        this.progressTracker = progressTracker;
        return this;
    }

    public RelationshipStreamExporterBuilder withArrowConnectionInfo(Optional<WriteConfig.ArrowConnectionInfo> arrowConnectionInfo, String databaseName) {
        this.arrowConnectionInfo = arrowConnectionInfo;
        this.databaseName = databaseName;
        return this;
    }
}
