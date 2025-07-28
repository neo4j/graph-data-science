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
package org.neo4j.gds.procedures;

import org.neo4j.gds.api.User;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;

import java.util.function.Predicate;

/**
 * An abstraction that allows us to stack off Neo4j concerns cleanly.
 */
public final class UserAccessor {

    private final Predicate<String> roleMatcher;

    private UserAccessor(Predicate<String> roleMatcher) {
        this.roleMatcher = roleMatcher;
    }

    public static UserAccessor create() {
        return new UserAccessor(role -> role.equals("admin"));
    }

    public static UserAccessor createForAuraDS() {
        // see https://github.com/neo-technology/neo4j-cloud/blob/master/components/database-role-definitions/k8s/base/cypher-roles.yaml for defined roles in Aura
        return new UserAccessor(role -> role.contains("_admin_") || role.equals("admin"));
    }

    public User getUser(SecurityContext securityContext) {
        AuthSubject subject = securityContext.subject();
        String username = subject.executingUser();
        boolean isAdmin = securityContext.roles().stream().anyMatch(roleMatcher);
        return new User(username, isAdmin);
    }
}
