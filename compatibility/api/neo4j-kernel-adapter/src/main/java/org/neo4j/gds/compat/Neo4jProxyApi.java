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
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;

import java.io.OutputStream;

public interface Neo4jProxyApi {

    @CompatSince(minor = 23)
    BatchImporter instantiateBatchImporter(
        DatabaseLayout dbLayout,
        FileSystemAbstraction fileSystem,
        ImportConfig config,
        Monitor monitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    );

    @CompatSince(minor = 23)
    ReadableGroups newGroups();

    @CompatSince(minor = 23)
    ReadableGroups newInitializedGroups();

    @CompatSince(minor = 23)
    Collector emptyCollector();

    @CompatSince(minor = 23)
    Collector badCollector(OutputStream outputStream, int batchSize);

    @CompatSince(minor = 23)
    Estimates knownEstimates(
        long numberOfNodes,
        long numberOfRelationships,
        long numberOfNodeProperties,
        long numberOfRelationshipProperties,
        long sizeOfNodeProperties,
        long sizeOfRelationshipProperties,
        long numberOfNodeLabels
    );

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    @CompatSince(minor = 24)
    void rethrowUnlessDuplicateRegistration(ProcedureException e) throws KernelException;

    @CompatSince(minor = 24)
    CallableProcedure callableProcedure(CompatCallableProcedure procedure);

    @CompatSince(minor = 24)
    int sequenceSizeAsInt(SequenceValue sequenceValue);

    @CompatSince(minor = 24)
    AnyValue sequenceValueAt(SequenceValue sequenceValue, int index);

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    @CompatSince(minor = 24)
    RuntimeException queryExceptionAsRuntimeException(Throwable e);

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    @CompatSince(minor = 24)
    ProcedureException procedureCallFailed(@PrintFormat String message, Object... args);

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    @CompatSince(minor = 24)
    ProcedureException procedureCallFailed(Throwable reason, @PrintFormat String message, Object... args);

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    @CompatSince(minor = 24)
    String exceptionMessage(Throwable e);

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    @CompatSince(minor = 24)
    DatabaseNotFoundException databaseNotFoundException(String message);
}
