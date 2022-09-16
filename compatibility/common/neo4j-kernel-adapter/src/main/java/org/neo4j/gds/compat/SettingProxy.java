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
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.graphdb.GraphDatabaseService;

public final class SettingProxy {

    private static final SettingProxyApi IMPL = ProxyUtil.findProxy(
        SettingProxyFactory.class,
        ProxyUtil.MayLogToStdout.NO
    );

    public static <T> Setting.Builder<T> newBuilder(
        String name,
        SettingValueParser<T> parser,
        @Nullable T defaultValue
    ) {
        return ImmutableSetting.builder(name, parser, defaultValue).convert(IMPL::setting);
    }

    public static DatabaseMode databaseMode(Config config, GraphDatabaseService databaseService) {
        return IMPL.databaseMode(config, databaseService);
    }

    @TestOnly
    public static void setDatabaseMode(Config config, DatabaseMode databaseMode, GraphDatabaseService databaseService) {
        IMPL.setDatabaseMode(config, databaseMode, databaseService);
    }

    private SettingProxy() {}
}
