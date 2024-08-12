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
package org.neo4j.gds.compat._518;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.compat.CompatLoginContext;
import org.neo4j.gds.compat.GlobalProcedureRegistry;
import org.neo4j.gds.compat.Neo4jProxyApi;
import org.neo4j.gds.compat.Write;
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
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.Mode;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.values.storable.Value;

import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

public final class Neo4jProxyImpl implements Neo4jProxyApi {

    @Override
    public BatchImporter instantiateBlockBatchImporter(
        DatabaseLayout dbLayout,
        FileSystemAbstraction fileSystem,
        ImportConfig config,
        Monitor monitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        return BatchImporterCompat.instantiateBlockBatchImporter(
            dbLayout,
            fileSystem,
            config,
            monitor,
            logService,
            dbConfig,
            jobScheduler,
            badCollector
        );
    }

    @Override
    public BatchImporter instantiateRecordBatchImporter(
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        ImportConfig config,
        ExecutionMonitor executionMonitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        return BatchImporterCompat.instantiateRecordBatchImporter(
            directoryStructure,
            fileSystem,
            config,
            executionMonitor,
            logService,
            dbConfig,
            jobScheduler,
            badCollector
        );
    }

    @Override
    public ExecutionMonitor newCoarseBoundedProgressExecutionMonitor(
        long highNodeId,
        long highRelationshipId,
        int batchSize,
        LongConsumer progress,
        LongConsumer outNumberOfBatches
    ) {
        return BatchImporterCompat.newCoarseBoundedProgressExecutionMonitor(
            highNodeId,
            highRelationshipId,
            batchSize,
            progress,
            outNumberOfBatches
        );
    }

    @Override
    public ReadableGroups newGroups() {
        return BatchImporterCompat.newGroups();
    }

    @Override
    public ReadableGroups newInitializedGroups() {
        return BatchImporterCompat.newInitializedGroups();
    }

    @Override
    public Collector emptyCollector() {
        return BatchImporterCompat.emptyCollector();
    }

    @Override
    public Collector badCollector(OutputStream outputStream, int batchSize) {
        return BatchImporterCompat.badCollector(outputStream, batchSize);
    }

    @Override
    public Estimates knownEstimates(
        long numberOfNodes,
        long numberOfRelationships,
        long numberOfNodeProperties,
        long numberOfRelationshipProperties,
        long sizeOfNodeProperties,
        long sizeOfRelationshipProperties,
        long numberOfNodeLabels
    ) {
        return BatchImporterCompat.knownEstimates(
            numberOfNodes,
            numberOfRelationships,
            numberOfNodeProperties,
            numberOfRelationshipProperties,
            sizeOfNodeProperties,
            sizeOfRelationshipProperties,
            numberOfNodeLabels
        );
    }

    @Override
    public GlobalProcedureRegistry globalProcedureRegistry(GlobalProcedures globalProcedures) {
        return new GlobalProcedureRegistry() {
            @Override
            public Stream<ProcedureSignature> getAllProcedures() {
                return globalProcedures.getCurrentView().getAllProcedures().stream();
            }

            @Override
            public Stream<UserFunctionSignature> getAllNonAggregatingFunctions() {
                return globalProcedures.getCurrentView().getAllNonAggregatingFunctions();
            }

            @Override
            public Stream<UserFunctionSignature> getAllAggregatingFunctions() {
                return globalProcedures.getCurrentView().getAllAggregatingFunctions();
            }
        };
    }

    @Override
    public Write dataWrite(KernelTransaction kernelTransaction) throws InvalidTransactionTypeKernelException {
        var neoWrite = kernelTransaction.dataWrite();
        return new Write() {

            @Override
            public void nodeAddLabel(long node, int nodeLabelToken) throws KernelException {
                neoWrite.nodeAddLabel(node, nodeLabelToken);
            }

            @Override
            public void nodeSetProperty(long node, int propertyKey, Value value) throws KernelException {
                neoWrite.nodeSetProperty(node, propertyKey, value);
            }

            @Override
            public long relationshipCreate(long source, int relationshipToken, long target) throws KernelException {
                return neoWrite.relationshipCreate(source, relationshipToken, target);
            }

            @Override
            public void relationshipSetProperty(long relationship, int propertyKey, Value value) throws
                KernelException {
                neoWrite.relationshipSetProperty(relationship, propertyKey, value);
            }
        };
    }

    @Override
    public ProcedureSignature procedureSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        List<FieldSignature> outputSignature,
        Mode mode,
        boolean admin,
        Optional<String> deprecatedBy,
        String description,
        String warning,
        boolean eager,
        boolean caseInsensitive,
        boolean systemProcedure,
        boolean internal,
        boolean allowExpiredCredentials,
        boolean threadSafe
    ) {
        var deprecated = deprecatedBy.filter(not(String::isEmpty));
        return new ProcedureSignature(
            name,
            inputSignature,
            outputSignature,
            mode,
            admin,
            deprecated.orElse(null),
            description,
            warning,
            eager,
            caseInsensitive,
            systemProcedure,
            internal,
            allowExpiredCredentials,
            threadSafe
        );
    }

    @Override
    public UserFunctionSignature userFunctionSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        Neo4jTypes.AnyType type,
        String description,
        Optional<String> deprecatedBy,
        boolean internal,
        boolean threadSafe
    ) {
        String category = null;      // No predefined category (like temporal or math)
        var caseInsensitive = false; // case sensitive name match
        var isBuiltIn = false;       // is built in; never true for GDS
        var deprecated = deprecatedBy.filter(not(String::isEmpty));

        return new UserFunctionSignature(
            name,
            inputSignature,
            type,
            deprecated.orElse(null),
            description,
            category,
            caseInsensitive,
            isBuiltIn,
            internal,
            threadSafe
        );
    }

    @Override
    public void relationshipProperties(
        Read read,
        long relationshipReference,
        long startNodeReference,
        Reference reference,
        PropertySelection selection,
        PropertyCursor cursor
    ) {
        read.relationshipProperties(relationshipReference, reference, selection, cursor);
    }

    @Override
    public LoginContext loginContext(CompatLoginContext compatLoginContext) {
        final class LoginContextImpl extends LoginContext {
            private final SecurityContext securityContext;

            private LoginContextImpl(CompatLoginContext compatLoginContext) {
                super(
                    compatLoginContext.subject(),
                    compatLoginContext.connectionInfo()
                );
                this.securityContext = compatLoginContext.securityContext();
            }

            @Override
            public SecurityContext authorize(IdLookup idLookup, String s, AbstractSecurityLog abstractSecurityLog) {
                return this.securityContext;
            }
        }

        return new LoginContextImpl(compatLoginContext);
    }
}
