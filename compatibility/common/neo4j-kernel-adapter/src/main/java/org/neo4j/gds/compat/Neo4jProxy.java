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

import org.intellij.lang.annotations.PrintFormat;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;

public final class Neo4jProxy {

    private static final Neo4jProxyApi IMPL = ProxyUtil.findProxy(
        Neo4jProxyFactory.class,
        ProxyUtil.MayLogToStdout.YES
    );

    public static <T> T lookupComponentProvider(Context ctx, Class<T> component, boolean safe)
        throws ProcedureException {
        var globalProcedures = GraphDatabaseApiProxy.resolveDependency(
            ctx.dependencyResolver(),
            GlobalProcedures.class
        );
        return globalProcedures.getCurrentView().lookupComponentProvider(component, safe).apply(ctx);
    }

    @SuppressForbidden(reason = "This is the compat API")
    public static CallableProcedure callableProcedure(CompatCallableProcedure procedure) {
        return IMPL.callableProcedure(procedure);
    }

    public static int sequenceSizeAsInt(SequenceValue listValue) {
        return IMPL.sequenceSizeAsInt(listValue);
    }

    public static AnyValue sequenceValueAt(SequenceValue sequenceValue, int index) {
        return IMPL.sequenceValueAt(sequenceValue, index);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static RuntimeException queryExceptionAsRuntimeException(Throwable e) {
        return IMPL.queryExceptionAsRuntimeException(e);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static ProcedureException procedureCallFailed(@PrintFormat String message, Object... args) {
        return IMPL.procedureCallFailed(message, args);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static ProcedureException procedureCallFailed(
        Throwable reason,
        @PrintFormat String message,
        Object... args
    ) {
        return IMPL.procedureCallFailed(reason, message, args);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static DatabaseNotFoundException databaseNotFoundException(String message) {
        return IMPL.databaseNotFoundException(message);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static String exceptionMessage(Throwable e) {
        return IMPL.exceptionMessage(e);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    static void rethrowUnlessDuplicateRegistration(ProcedureException e) throws KernelException {
        IMPL.rethrowUnlessDuplicateRegistration(e);
    }

    private Neo4jProxy() {
        throw new UnsupportedOperationException("No instances");
    }
}
