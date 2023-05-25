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
package com.neo4j.gds.internal;

import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

public final class CustomProceduresUtil {

    public static <T> T resolveDependency(Context ctx, Class<T> dependency) {
        return GraphDatabaseApiProxy.resolveDependency(ctx.dependencyResolver(), dependency);
    }

    public static <T> T lookupComponentProvider(Context ctx, Class<T> component) throws ProcedureException {
        var globalProcedures = resolveDependency(ctx, GlobalProcedures.class);
        return globalProcedures.lookupComponentProvider(component, false).apply(ctx);
    }

    /**
     * Like {@link #lookupComponentProvider(Context, Class)} but only allows safe components.
     * Safe components are those that are not sandboxed by the kernel and can be used without
     * setting the {@code dbms.security.procedures.unrestricted} setting.
     */
    public static <T> T lookupSafeComponentProvider(Context ctx, Class<T> component) throws ProcedureException {
        var globalProcedures = resolveDependency(ctx, GlobalProcedures.class);
        return globalProcedures.lookupComponentProvider(component, true).apply(ctx);
    }

    private CustomProceduresUtil() {}
}
