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
package org.neo4j.gds.core.utils.warnings;

import org.neo4j.gds.utils.StringFormatting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @deprecated this is a temporary workaround
 */
@Deprecated
public final class UserLogStoreHolder {
    /**
     * We need to satisfy each procedure facade having its own user log stores, so that we can new them up in a known,
     * good, isolated state.
     * Plus for the time being we have to ensure each test using a user log store sees unique user log stores.
     * We assume database ids are unique to a JVM.
     * Therefore, we can have this JVM-wide singleton holding a map of database id to user log store.
     * <p>
     * This is of course abominable and should be gotten rid of.
     * And we can get rid of it once all tests are not using context-injected user log stores anymore.
     * We want to do that migration in vertical slices, so we can have this solution in place temporarily.
     * Procedure facade will use this service class and be oblivious to the underlying abomination.
     * And UserLogRegistryExtension will be made to hand out references using database id.
     * We rely on database ids being unique for the lifetime of a JVM.
     * <p>
     * Note that UserLogRegistryExtension will thus serve both tests and prod,
     * until we can eliminate usages of its products, and in turn eliminate it completely.
     * <p>
     * Oh, and string type because this module doesn't know the GDS DatabaseId type.
     *
     * @deprecated we eliminate this as soon as possible
     */
    @Deprecated
    private static final Map<String, GlobalUserLogStore> USER_LOG_STORES = new ConcurrentHashMap<>();

    private UserLogStoreHolder() {}

    /**
     * Normalize so that we match things from the GDS DatabaseId, and fingers crossed it doesn't change.
     * Not using DatabaseId directly, because that would mead to some awful dependencies.
     * And we will eliminate this in due course.
     */
    public static GlobalUserLogStore getUserLogStore(String databaseName) {
        String normalizedDatabaseName = StringFormatting.toLowerCaseWithLocale(databaseName);

        return USER_LOG_STORES.computeIfAbsent(normalizedDatabaseName, __ -> new GlobalUserLogStore());
    }
}
