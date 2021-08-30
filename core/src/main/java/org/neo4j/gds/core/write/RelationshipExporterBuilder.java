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
import org.neo4j.gds.api.IdMapping;
import org.neo4j.gds.core.TransactionContext;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.Values;

import java.util.Objects;
import java.util.function.LongUnaryOperator;

public abstract class RelationshipExporterBuilder<T extends RelationshipExporter> {

    static final int DEFAULT_WRITE_CONCURRENCY = 1;

    protected final TransactionContext transactionContext;
    protected LongUnaryOperator toOriginalId;
    protected TerminationFlag terminationFlag;

    protected Graph graph;

    protected ProgressLogger progressLogger;

    RelationshipPropertyTranslator propertyTranslator;

    RelationshipExporterBuilder(TransactionContext transactionContext) {
        this.transactionContext = Objects.requireNonNull(transactionContext);
        this.propertyTranslator = Values::doubleValue;

        this.progressLogger = ProgressLogger.NULL_LOGGER;
    }

    public abstract T build();

    public RelationshipExporterBuilder<T> withRelationPropertyTranslator(RelationshipPropertyTranslator propertyTranslator) {
        this.propertyTranslator = propertyTranslator;
        return this;
    }

    public RelationshipExporterBuilder<T> withGraph(Graph graph) {
        this.graph = graph;
        return this;
    }

    public RelationshipExporterBuilder<T> withIdMapping(IdMapping idMapping) {
        Objects.requireNonNull(idMapping);
        this.toOriginalId = idMapping::toOriginalNodeId;
        return this;
    }

    public RelationshipExporterBuilder<T> withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    public RelationshipExporterBuilder<T> withLog(Log log) {
        return withProgressLogger(new BatchingProgressLogger(
            log,
            Tasks.leaf(taskName(), graph.relationshipCount()),
            DEFAULT_WRITE_CONCURRENCY
        ));
    }

    private RelationshipExporterBuilder<T> withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }

    protected String taskName() {
        return "WriteRelationships";
    }
}
