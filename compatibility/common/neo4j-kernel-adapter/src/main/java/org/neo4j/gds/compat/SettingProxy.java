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
package org.neo4j.gds.compat;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.configuration.SettingBuilder;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class SettingProxy {

    public static <T> SettingBuilder<T> newBuilder(
        String name,
        SettingValueParser<T> parser,
        @Nullable T defaultValue
    ) {
        return SettingBuilder.newBuilder(name, parser, defaultValue);
    }

    public static DatabaseMode databaseMode(GraphDatabaseService databaseService) {
        return switch (((GraphDatabaseAPI) databaseService).mode()) {
            case RAFT -> DatabaseMode.CORE;
            case REPLICA -> DatabaseMode.READ_REPLICA;
            case SINGLE -> DatabaseMode.SINGLE;
            case VIRTUAL -> DatabaseMode.VIRTUAL;
        };
    }

    @TestOnly
    public static void setDatabaseMode(DatabaseMode databaseMode, GraphDatabaseService databaseService) {
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
                case VIRTUAL -> TopologyGraphDbmsModel.HostedOnMode.VIRTUAL;
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

    public static String secondaryModeName() {
        return "Secondary";
    }

    private SettingProxy() {}
}
