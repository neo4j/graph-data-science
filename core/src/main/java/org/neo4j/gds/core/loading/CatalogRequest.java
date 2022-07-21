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
package org.neo4j.gds.core.loading;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Optional;

@ValueClass
public abstract class CatalogRequest {

    public final String username() {
        return usernameOverride().orElseGet(this::requestingUsername);
    }

    public abstract String databaseName();

    public final boolean restrictSearchToUsernameCatalog() {
        // admin users are allowed to not have a graph, other users graphs are then searched
        if (requesterIsAdmin() && usernameOverride().isEmpty()) {
            return false;
        }
        return true;
    }

    abstract String requestingUsername();

    abstract Optional<String> usernameOverride();

    abstract boolean requesterIsAdmin();

    @Value.Check
    final void validate() {
        if (!requesterIsAdmin() && usernameOverride().isPresent()) {
            throw new IllegalStateException("Cannot override the username as a non-admin");
        }
    }

    public static CatalogRequest of(String username, String databaseName) {
        return ImmutableCatalogRequest.of(databaseName, username, Optional.empty(), false);
    }

    public static CatalogRequest of(String username, NamedDatabaseId databaseId) {
        return of(username, databaseId.name());
    }

    public static CatalogRequest of(String username, DatabaseId databaseId) {
        return of(username, databaseId.databaseName());
    }

    public static CatalogRequest ofAdmin(String username, String databaseName) {
        return ofAdmin(username, Optional.empty(), databaseName);
    }

    public static CatalogRequest ofAdmin(String username, Optional<String> usernameOverride, String databaseName) {
        return ImmutableCatalogRequest.of(databaseName, username, usernameOverride, true);
    }

    public static CatalogRequest ofAdmin(String username, NamedDatabaseId databaseId) {
        return ofAdmin(username, databaseId.name());
    }

    public static CatalogRequest ofAdmin(String username, Optional<String> usernameOverride, NamedDatabaseId databaseId) {
        return ofAdmin(username, usernameOverride, databaseId.name());
    }
}
