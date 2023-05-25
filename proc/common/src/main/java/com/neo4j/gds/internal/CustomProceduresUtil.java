/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
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
