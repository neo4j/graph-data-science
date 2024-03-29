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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.annotation.Parameters;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IndexConfig;

import java.util.Optional;

@Parameters
public final class GraphStoreToDatabaseExporterParameters {
    private final String dbName;
    private final int writeConcurrency;
    private final int batchSize;
    private final String defaultRelationshipType;
    private final boolean enableDebugLog;

    private String recordFormat;
    private boolean useBadCollector;
    private boolean highIO;
    private boolean force;
    private Optional<Long> pageCacheMemory;

    public static GraphStoreToDatabaseExporterParameters create(
        String dbName,
        int writeConcurrency,
        int batchSize,
        boolean enableDebugLog,
        @NotNull String defaultRelationshipType
    ) {
        var defaultRecordFormat = Neo4jProxy.defaultRecordFormatSetting();
        var defaultUseBadCollector = false;
        return new GraphStoreToDatabaseExporterParameters(
            dbName,
            writeConcurrency,
            batchSize,
            defaultRelationshipType,
            enableDebugLog,
            defaultRecordFormat,
            defaultUseBadCollector
        );
    }

    private GraphStoreToDatabaseExporterParameters(
        String dbName,
        int writeConcurrency,
        int batchSize,
        String defaultRelationshipType,
        boolean enableDebugLog,
        String recordFormat,
        boolean useBadCollector
    ) {
        this.dbName = dbName;
        this.writeConcurrency = writeConcurrency;
        this.batchSize = batchSize;
        this.defaultRelationshipType = defaultRelationshipType;
        this.enableDebugLog = enableDebugLog;

        this.recordFormat = recordFormat;
        this.useBadCollector = useBadCollector;

        this.highIO = false;
        this.force = false;
        this.pageCacheMemory = Optional.empty();
    }

    String dbName() {
        return dbName;
    }

    String recordFormat() {
        return recordFormat;
    }

    boolean useBadCollector() {
        return useBadCollector;
    }

    boolean force() {
        return force;
    }

    boolean enableDebugLog() {
        return enableDebugLog;
    }

    int writeConcurrency() {
        return writeConcurrency;
    }

    int batchSize() {
        return batchSize;
    }

    String defaultRelationshipType() {
        return defaultRelationshipType;
    }

    public GraphStoreToDatabaseExporterParameters withPageCacheMemory(long pageCacheMemory) {
        this.pageCacheMemory = Optional.of(pageCacheMemory);
        return this;
    }

    public GraphStoreToDatabaseExporterParameters withForce(boolean force) {
        this.force = force;
        return this;
    }

    public GraphStoreToDatabaseExporterParameters withHighIO(boolean highIO) {
        this.highIO = highIO;
        return this;
    }

    public GraphStoreToDatabaseExporterParameters andAlsoWithUseBadCollector(boolean useBadCollector) {
        this.useBadCollector = useBadCollector;
        return this;
    }

    public GraphStoreToDatabaseExporterParameters andAlsoWithRecordFormat(String recordFormat) {
        this.recordFormat = recordFormat;
        return this;
    }

    Configuration toBatchImporterConfig() {
        return Neo4jProxy.batchImporterConfig(
            batchSize,
            writeConcurrency,
            highIO,
            IndexConfig.DEFAULT.withLabelIndex().withRelationshipTypeIndex()
        );
    }
}
