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

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;

import java.util.Optional;

public record CatalogRequest(
    String databaseName,
    String requestingUsername,
    Optional<String> usernameOverride,
    boolean requesterIsAdmin
) {
    public CatalogRequest{
        if (!requesterIsAdmin && usernameOverride.isPresent()) {
            throw new IllegalStateException("Cannot override the username as a non-admin");
        }
    }

    public String username() {
        return usernameOverride().orElseGet(this::requestingUsername);
    }

    boolean restrictSearchToUsernameCatalog() {
        // admin users are allowed to not have a graph, other users graphs are then searched
        return !requesterIsAdmin() || usernameOverride().isPresent();
    }

    public static CatalogRequest of(User user, DatabaseId databaseId) {
        return new CatalogRequest(databaseId.databaseName(), user.getUsername(), Optional.empty(), user.isAdmin());
    }

    public static CatalogRequest of(String username, String databaseName) {
        return new CatalogRequest(databaseName, username, Optional.empty(), false);
    }

    public static CatalogRequest of(String username, DatabaseId databaseId) {
        return of(username, databaseId.databaseName());
    }
}
