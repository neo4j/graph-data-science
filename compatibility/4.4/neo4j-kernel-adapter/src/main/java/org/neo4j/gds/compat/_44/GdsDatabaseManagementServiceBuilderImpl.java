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
package org.neo4j.gds.compat._44;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.gds.compat.GdsDatabaseManagementServiceBuilder;
import org.neo4j.graphdb.config.Setting;

import java.nio.file.Path;
import java.util.Map;

final class GdsDatabaseManagementServiceBuilderImpl implements GdsDatabaseManagementServiceBuilder {

    private final DatabaseManagementServiceBuilder dbmsBuilder;

    GdsDatabaseManagementServiceBuilderImpl(Path storeDir) {
        this.dbmsBuilder = new DatabaseManagementServiceBuilder(storeDir);
    }

    @Override
    public GdsDatabaseManagementServiceBuilder setConfigRaw(Map<String, String> configMap) {
        dbmsBuilder.setConfigRaw(configMap);
        return this;
    }

    @Override
    public <S> GdsDatabaseManagementServiceBuilder setConfig(Setting<S> setting, S value) {
        dbmsBuilder.setConfig(setting, value);
        return this;
    }

    @Override
    public DatabaseManagementService build() {
        return dbmsBuilder.build();
    }
}
