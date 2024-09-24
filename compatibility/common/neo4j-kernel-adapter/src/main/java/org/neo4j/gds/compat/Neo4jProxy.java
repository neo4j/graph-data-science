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
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.compat.batchimport.BatchImporter;
import org.neo4j.gds.compat.batchimport.ImportConfig;
import org.neo4j.gds.compat.batchimport.Monitor;
import org.neo4j.gds.compat.batchimport.input.Collector;
import org.neo4j.gds.compat.batchimport.input.Estimates;
import org.neo4j.gds.compat.batchimport.input.ReadableGroups;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.SequenceValue;

import java.io.OutputStream;

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

    public static BatchImporter instantiateBatchImporter(
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        ImportConfig configuration,
        LogService logService,
        Monitor executionMonitor,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        return IMPL.instantiateBatchImporter(
            directoryStructure,
            fileSystem,
            configuration,
            executionMonitor,
            logService,
            dbConfig,
            jobScheduler,
            badCollector
        );
    }

    public static ReadableGroups newGroups() {
        return IMPL.newGroups();
    }

    public static ReadableGroups newInitializedGroups() {
        return IMPL.newInitializedGroups();
    }

    public static Collector emptyCollector() {
        return IMPL.emptyCollector();
    }

    public static Collector badCollector(OutputStream log, int batchSize) {
        return IMPL.badCollector(log, batchSize);
    }

    public static Estimates knownEstimates(
        long numberOfNodes,
        long numberOfRelationships,
        long numberOfNodeProperties,
        long numberOfRelationshipProperties,
        long sizeOfNodeProperties,
        long sizeOfRelationshipProperties,
        long numberOfNodeLabels
    ) {
        return IMPL.knownEstimates(
            numberOfNodes,
            numberOfRelationships,
            numberOfNodeProperties,
            numberOfRelationshipProperties,
            sizeOfNodeProperties,
            sizeOfRelationshipProperties,
            numberOfNodeLabels
        );
    }

    @SuppressForbidden(reason = "This is the compat API")
    public static CallableProcedure callableProcedure(CompatCallableProcedure procedure) {
        return IMPL.callableProcedure(procedure);
    }

    public static int sequenceSizeAsInt(SequenceValue listValue) {
        return IMPL.sequenceSizeAsInt(listValue);
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
