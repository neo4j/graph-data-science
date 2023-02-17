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
package org.neo4j.gds.compat._55;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingBuilder;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.gds.compat.DatabaseMode;
import org.neo4j.gds.compat.SettingProxyApi;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class SettingProxyImpl implements SettingProxyApi {

    @Override
    public <T> Setting<T> setting(org.neo4j.gds.compat.Setting<T> setting) {
        var builder = SettingBuilder.newBuilder(setting.name(), setting.parser(), setting.defaultValue());
        if (setting.dynamic()) {
            builder = builder.dynamic();
        }
        if (setting.immutable()) {
            builder = builder.immutable();
        }
        setting.dependency().ifPresent(builder::setDependency);
        setting.constraints().forEach(builder::addConstraint);
        return builder.build();
    }

    @Override
    public DatabaseMode databaseMode(Config config, GraphDatabaseService databaseService) {
        return switch (((GraphDatabaseAPI) databaseService).mode()) {
            case RAFT -> DatabaseMode.CORE;
            case REPLICA -> DatabaseMode.READ_REPLICA;
            case SINGLE -> DatabaseMode.SINGLE;
            case VIRTUAL -> throw new UnsupportedOperationException("What's a virtual database anyway?");
        };
    }

    @Override
    public void setDatabaseMode(Config config, DatabaseMode databaseMode, GraphDatabaseService databaseService) {
        // super hacky, there is no way to set the mode of a database without restarting it
        if (!(databaseService instanceof GraphDatabaseFacade db)) {
            throw new IllegalArgumentException(
                "Cannot set database mode on a database that is not a GraphDatabaseFacade");
        }
        try {
            var modeField = GraphDatabaseFacade.class.getDeclaredField("mode");
            modeField.setAccessible(true);
            modeField.set(db, switch (databaseMode) {
                case CORE -> TopologyGraphDbmsModel.HostedOnMode.RAFT;
                case READ_REPLICA -> TopologyGraphDbmsModel.HostedOnMode.REPLICA;
                case SINGLE -> TopologyGraphDbmsModel.HostedOnMode.SINGLE;
            });
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(
                "Could not set the mode field because it no longer exists. This compat layer needs to be updated.",
                e
            );
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not get the permissions to set the mode field.", e);
        }
    }

    @Override
    public String secondaryModeName() {
        return "Secondary";
    }
}
