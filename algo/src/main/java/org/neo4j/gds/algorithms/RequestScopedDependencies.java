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
package org.neo4j.gds.algorithms;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.termination.TerminationFlag;

/**
 * This is a handy class for transporting similar dependencies through layers.
 * And especially useful when that list grows or shrinks - less sites to edit innit
 */
public final class RequestScopedDependencies {
    private final DatabaseId databaseId;
    private final TerminationFlag terminationFlag;
    private final User user;

    /**
     * Over-doing it with a private constructor?
     * <p>
     * I just really like the <code>RequestScopedDependencies.builder().build()</code> form
     */
    private RequestScopedDependencies(
        DatabaseId databaseId,
        TerminationFlag terminationFlag,
        User user
    ) {
        this.databaseId = databaseId;
        this.terminationFlag = terminationFlag;
        this.user = user;
    }

    public static RequestScopedDependenciesBuilder builder() {
        return new RequestScopedDependenciesBuilder();
    }

    public DatabaseId getDatabaseId() {
        return databaseId;
    }

    public TerminationFlag getTerminationFlag() {
        return terminationFlag;
    }

    public User getUser() {
        return user;
    }

    public static class RequestScopedDependenciesBuilder {
        private DatabaseId databaseId = DatabaseId.DEFAULT;
        private User user = User.DEFAULT;
        private TerminationFlag terminationFlag = TerminationFlag.DEFAULT;

        public RequestScopedDependenciesBuilder with(DatabaseId databaseId) {
            this.databaseId = databaseId;
            return this;
        }

        public RequestScopedDependenciesBuilder with(TerminationFlag terminationFlag) {
            this.terminationFlag = terminationFlag;
            return this;
        }

        public RequestScopedDependenciesBuilder with(User user) {
            this.user = user;
            return this;
        }

        public RequestScopedDependencies build() {
            return new RequestScopedDependencies(databaseId, terminationFlag, user);
        }
    }
}
