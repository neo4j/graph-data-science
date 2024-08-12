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

import org.jetbrains.annotations.Nullable;
import org.neo4j.configuration.Config;
import org.neo4j.gds.compat.batchimport.BatchImporter;
import org.neo4j.gds.compat.batchimport.ExecutionMonitor;
import org.neo4j.gds.compat.batchimport.ImportConfig;
import org.neo4j.gds.compat.batchimport.Monitor;
import org.neo4j.gds.compat.batchimport.input.Collector;
import org.neo4j.gds.compat.batchimport.input.Estimates;
import org.neo4j.gds.compat.batchimport.input.ReadableGroups;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.Mode;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;

import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.function.LongConsumer;

public interface Neo4jProxyApi {

    @CompatSince(minor = 21)
    GlobalProcedureRegistry globalProcedureRegistry(GlobalProcedures globalProcedures);

    @CompatSince(minor = 21)
    Write dataWrite(KernelTransaction kernelTransaction) throws InvalidTransactionTypeKernelException;

    @CompatSince(minor = 21)
    ProcedureSignature procedureSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        List<FieldSignature> outputSignature,
        Mode mode,
        boolean admin,
        Optional<String> deprecatedBy,
        String description,
        @Nullable String warning,
        boolean eager,
        boolean caseInsensitive,
        boolean systemProcedure,
        boolean internal,
        boolean allowExpiredCredentials,
        boolean threadSafe
    );

    @CompatSince(minor = 21)
    UserFunctionSignature userFunctionSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        Neo4jTypes.AnyType type,
        String description,
        Optional<String> deprecatedBy,
        boolean internal,
        boolean threadSafe
    );

    @CompatSince(minor = 22)
    void relationshipProperties(
        Read read,
        long relationshipReference,
        long startNodeReference,
        Reference reference,
        PropertySelection selection,
        PropertyCursor cursor
    );

    @CompatSince(minor = 23)
    BatchImporter instantiateBlockBatchImporter(
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
    BatchImporter instantiateRecordBatchImporter(
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        ImportConfig config,
        ExecutionMonitor executionMonitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    );

    @CompatSince(minor = 23)
    default ExecutionMonitor newCoarseBoundedProgressExecutionMonitor(
        long highNodeId,
        long highRelationshipId,
        int batchSize,
        LongConsumer progress,
        LongConsumer outNumberOfBatches
    ) {
        throw new UnsupportedOperationException(
            "`org.neo4j.gds.compat._519.Neo4jProxyApi.newCoarseBoundedProgressExecutionMonitor` is not yet implemented.");
    }

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

    @CompatSince(minor = 23)
    LoginContext loginContext(CompatLoginContext compatLoginContext);
}
