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

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.JobIdConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.io.GraphStoreExporterBaseConfig;
import org.neo4j.internal.batchimport.IndexConfig;

import java.util.Optional;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface GraphStoreToDatabaseExporterConfig extends GraphStoreExporterBaseConfig, JobIdConfig {

    String DB_NAME_KEY = "dbName";

    @Configuration.Key(DB_NAME_KEY)
    String dbName();

    @Value.Default
    default boolean enableDebugLog() {
        return false;
    }

    @Value.Default
    @Configuration.Ignore
    @SuppressWarnings("immutables:untype")
    default long executionMonitorCheckMillis() {
        return 200;
    }

    @Value.Default
    @Configuration.Ignore
    @SuppressWarnings("immutables:untype")
    default Optional<Long> pageCacheMemory() {
        return Optional.empty();
    }

    @Value.Default
    @Configuration.Ignore
    @SuppressWarnings("immutables:untype")
    default String recordFormat() {
        return Neo4jProxy.defaultRecordFormatSetting();
    }

    @Value.Default
    @Configuration.Ignore
    default boolean force() {
        return false;
    }

    @Value.Default
    @Configuration.Ignore
    default boolean highIO() {
        return false;
    }

    @Value.Default
    @Configuration.Ignore
    default boolean useBadCollector() {
        return false;
    }

    @Value.Check
    default void validate() {
        Neo4jProxy.validateExternalDatabaseName(dbName());
    }

    static GraphStoreToDatabaseExporterConfig of(CypherMapWrapper config) {
        var normalizedConfig = config.getString(DB_NAME_KEY).map(dbName -> {
            var databaseName = Neo4jProxy.validateExternalDatabaseName(dbName);
            return config.withString(DB_NAME_KEY, databaseName);
        }).orElse(config);
        return new GraphStoreToDatabaseExporterConfigImpl(normalizedConfig);
    }

    default org.neo4j.internal.batchimport.Configuration toBatchImporterConfig() {
        var exportConfig = this;
        return new org.neo4j.internal.batchimport.Configuration() {
            @Override
            public int batchSize() {
                return exportConfig.batchSize();
            }

            @Override
            public int maxNumberOfWorkerThreads() {
                return exportConfig.writeConcurrency();
            }

            @Override
            public long pageCacheMemory() {
                return exportConfig
                    .pageCacheMemory()
                    .orElseGet(org.neo4j.internal.batchimport.Configuration.super::pageCacheMemory);
            }

            @Override
            public boolean highIO() {
                return exportConfig.highIO();
            }

            @Override
            public IndexConfig indexConfig() {
                return IndexConfig.DEFAULT.withLabelIndex();
            }
        };
    }
}
