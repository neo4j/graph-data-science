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
package org.neo4j.gds;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminAccessMode;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.DatabaseAccessMode;
import org.neo4j.internal.kernel.api.security.PermissionState;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;

import java.util.Set;

/**
 * Let's build this out as needed.
 * It needs to be just good enough for what we need through ProcedureRunner, nothing more
 */
class FakeSecurityContext extends SecurityContext {
    FakeSecurityContext() {
        super(null, null, null, null);
    }

    @Override
    public AccessMode mode() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public DatabaseAccessMode databaseAccessMode() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public String database() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public PermissionState allowExecuteAdminProcedure(int procedureId) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public PermissionState allowsAdminAction(AdminActionOnResource action) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Set<String> roles() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public SecurityContext authorize(IdLookup idLookup, String dbName, AbstractSecurityLog securityLog) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public SecurityContext withMode(AccessMode mode) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public SecurityContext withMode(AdminAccessMode adminAccessMode) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void assertCredentialsNotExpired(SecurityAuthorizationHandler handler) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public String description() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    protected String defaultString(String name) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public AuthSubject subject() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public ClientConnectionInfo connectionInfo() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public boolean impersonating() {
        throw new UnsupportedOperationException("TODO");
    }
}
