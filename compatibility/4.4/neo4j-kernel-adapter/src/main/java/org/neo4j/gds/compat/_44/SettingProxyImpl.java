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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.compat.DatabaseMode;
import org.neo4j.gds.compat.SettingProxyApi;
import org.neo4j.graphdb.config.Setting;

public class SettingProxyImpl implements SettingProxyApi {
    @Override
    @SuppressForbidden(reason = "This is the compat specific use")
    public <T> Setting<T> setting(org.neo4j.gds.compat.Setting<T> setting) {
        var builder = SettingImpl.newBuilder(setting.name(), setting.parser(), setting.defaultValue());
        if (setting.dynamic()) {
            builder = builder.dynamic();
        }
        setting.constraints().forEach(builder::addConstraint);
        return builder.build();
    }

    @Override
    public DatabaseMode databaseMode(Config config) {
        var mode = config.get(GraphDatabaseSettings.mode);
        switch (mode) {
            case SINGLE:
                return DatabaseMode.SINGLE;
            case CORE:
                return DatabaseMode.CORE;
            case READ_REPLICA:
                return DatabaseMode.READ_REPLICA;
            default:
                throw new IllegalStateException("Unexpected value: " + mode);
        }
    }

    @Override
    public void setDatabaseMode(Config config, DatabaseMode databaseMode) {
        GraphDatabaseSettings.Mode mode;
        switch (databaseMode) {
            case SINGLE:
                mode = GraphDatabaseSettings.Mode.SINGLE;
                break;
            case CORE:
                mode = GraphDatabaseSettings.Mode.CORE;
                break;
            case READ_REPLICA:
                mode = GraphDatabaseSettings.Mode.READ_REPLICA;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + databaseMode);
        }
        config.set(GraphDatabaseSettings.mode, mode);
    }
}
