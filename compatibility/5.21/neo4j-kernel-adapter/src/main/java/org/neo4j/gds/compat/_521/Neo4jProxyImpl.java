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
package org.neo4j.gds.compat._521;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.compat.CompatExecutionMonitor;
import org.neo4j.gds.compat.CompatMonitor;
import org.neo4j.gds.compat.GlobalProcedureRegistry;
import org.neo4j.gds.compat.Neo4jProxyApi;
import org.neo4j.gds.compat.Write;
import org.neo4j.gds.compat.batchimport.BatchImporter;
import org.neo4j.gds.compat.batchimport.Configuration;
import org.neo4j.gds.compat.batchimport.Monitor;
import org.neo4j.gds.compat.batchimport.input.Collector;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.Mode;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.values.storable.Value;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

public final class Neo4jProxyImpl implements Neo4jProxyApi {

    private CursorContextFactory cursorContextFactory(Optional<PageCacheTracer> pageCacheTracer) {
        return pageCacheTracer.map(cacheTracer -> new CursorContextFactory(
            cacheTracer,
            FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER
        )).orElse(CursorContextFactory.NULL_CONTEXT_FACTORY);
    }

    @Override
    public BatchImporter instantiateBlockBatchImporter(
        DatabaseLayout databaseLayout,
        FileSystemAbstraction fileSystem,
        PageCacheTracer pageCacheTracer,
        Configuration configuration,
        CompatMonitor compatMonitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        var storageEngineFactory = StorageEngineFactory.selectStorageEngine(dbConfig);
        var progressOutput = new PrintStream(PrintStream.nullOutputStream(), true, StandardCharsets.UTF_8);
        var verboseProgressOutput = false;

        throw new UnsupportedOperationException("TODO");
//        return storageEngineFactory.batchImporter(
//            databaseLayout,
//            fileSystem,
//            pageCacheTracer,
//            configuration,
//            logService,
//            progressOutput,
//            verboseProgressOutput,
//            additionalInitialIds,
//            dbConfig,
//            toMonitor(compatMonitor),
//            jobScheduler,
//            badCollector,
//            TransactionLogInitializer.getLogFilesInitializer(),
//            new IndexImporterFactoryImpl(),
//            EmptyMemoryTracker.INSTANCE,
//            cursorContextFactory(Optional.empty())
//        );
    }

    @Override
    public BatchImporter instantiateRecordBatchImporter(
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        PageCacheTracer aNull,
        Configuration configuration,
        CompatExecutionMonitor compatExecutionMonitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        throw new UnsupportedOperationException(
            "`org.neo4j.gds.compat._521.Neo4jProxyImpl.instantiateRecordBatchImporter` is not yet implemented.");
    }

    private static Monitor toMonitor(CompatMonitor compatMonitor) {
        return new Monitor() {
            @Override
            public void started() {
                compatMonitor.started();
            }

            @Override
            public void percentageCompleted(int percentage) {
                compatMonitor.percentageCompleted(percentage);
            }

            @Override
            public void completed(boolean success) {
                compatMonitor.completed(success);
            }
        };
    }

    @Override
    public GlobalProcedureRegistry globalProcedureRegistry(GlobalProcedures globalProcedures) {
        return new GlobalProcedureRegistry() {
            @Override
            public Stream<ProcedureSignature> getAllProcedures() {
                return globalProcedures.getCurrentView().getAllProcedures(CypherScope.CYPHER_5);
            }

            @Override
            public Stream<UserFunctionSignature> getAllNonAggregatingFunctions() {
                return globalProcedures.getCurrentView().getAllNonAggregatingFunctions(CypherScope.CYPHER_5);
            }

            @Override
            public Stream<UserFunctionSignature> getAllAggregatingFunctions() {
                return globalProcedures.getCurrentView().getAllAggregatingFunctions(CypherScope.CYPHER_5);
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
            deprecated.isPresent(),
            deprecated.orElse(null),
            description,
            warning,
            eager,
            caseInsensitive,
            systemProcedure,
            internal,
            allowExpiredCredentials,
            threadSafe,
            CypherScope.ALL_SCOPES
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
        String category = null;      // No predefined categpry (like temporal or math)
        var caseInsensitive = false; // case sensitive name match
        var isBuiltIn = false;       // is built in; never true for GDS
        var deprecated = deprecatedBy.filter(not(String::isEmpty));

        return new UserFunctionSignature(
            name,
            inputSignature,
            type,
            deprecated.isPresent(),
            deprecated.orElse(null),
            description,
            category,
            caseInsensitive,
            isBuiltIn,
            internal,
            threadSafe,
            CypherScope.ALL_SCOPES
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
}
