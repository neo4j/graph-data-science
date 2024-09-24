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

import org.neo4j.configuration.helpers.DatabaseNameValidator;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.compat.SettingProxy;
import org.neo4j.gds.config.JobIdConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.io.GraphStoreExporterBaseConfig;
import org.neo4j.kernel.database.NormalizedDatabaseName;

@Configuration
public interface GraphStoreToDatabaseExporterConfig extends GraphStoreExporterBaseConfig, JobIdConfig {

    String DB_NAME_KEY = "dbName";
    String DB_FORMAT_KEY = "dbFormat";

    @Configuration.Key(DB_NAME_KEY)
    String databaseName();

    @Deprecated(forRemoval = true, since = "2.2")
    default boolean enableDebugLog() {
        return false;
    }

    @Configuration.Key(DB_FORMAT_KEY)
    default String databaseFormat() {
        return SettingProxy.defaultDatabaseFormatSetting();
    }

    @Configuration.Check
    default void validate() {
        var normalizedName = new NormalizedDatabaseName(databaseName());
        DatabaseNameValidator.validateExternalDatabaseName(normalizedName);
    }

    static GraphStoreToDatabaseExporterConfig of(CypherMapWrapper config) {
        var normalizedConfig = config.getString(DB_NAME_KEY).map(dbName -> {
            var normalizedName = new NormalizedDatabaseName(dbName);
            DatabaseNameValidator.validateExternalDatabaseName(normalizedName);
            var databaseName = normalizedName.name();
            return config.withString(DB_NAME_KEY, databaseName);
        }).orElse(config);
        return new GraphStoreToDatabaseExporterConfigImpl(normalizedConfig);
    }
}
