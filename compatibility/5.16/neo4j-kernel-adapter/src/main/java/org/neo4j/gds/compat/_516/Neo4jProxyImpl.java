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
package org.neo4j.gds.compat._516;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.compat.GlobalProcedureRegistry;
import org.neo4j.gds.compat.Neo4jProxyApi;
import org.neo4j.gds.compat.Write;
import org.neo4j.gds.compat.batchimport.BatchImporter;
import org.neo4j.gds.compat.batchimport.ExecutionMonitor;
import org.neo4j.gds.compat.batchimport.input.Collector;
import org.neo4j.gds.compat.batchimport.input.Input;
import org.neo4j.gds.compat.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IndexConfig;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.input.Collectors;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.PropertySizeCalculator;
import org.neo4j.internal.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.internal.batchimport.staging.StageExecution;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.procedure.Mode;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

public final class Neo4jProxyImpl implements Neo4jProxyApi {

    @Override
    public BatchImporter instantiateBlockBatchImporter(
        DatabaseLayout dbLayout,
        FileSystemAbstraction fileSystem,
        org.neo4j.gds.compat.batchimport.Config config,
        org.neo4j.gds.compat.batchimport.Monitor monitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        throw new UnsupportedOperationException(
            "GDS does not support block store format batch importer on this Neo4j version. Requires >= Neo4j 5.18.");
    }

    @Override
    public BatchImporter instantiateRecordBatchImporter(
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        org.neo4j.gds.compat.batchimport.Config config,
        ExecutionMonitor executionMonitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        var importer = BatchImporterFactory.withHighestPriority()
            .instantiate(
                directoryStructure,
                fileSystem,
                PageCacheTracer.NULL,
                new ConfigurationAdapter(config),
                logService,
                new ExecutionMonitorAdapter(executionMonitor),
                AdditionalInitialIds.EMPTY,
                new EmptyLogTailMetadata(dbConfig),
                dbConfig,
                Monitor.NO_MONITOR,
                jobScheduler,
                badCollector != null ? ((CollectorAdapter) badCollector).inner : null,
                TransactionLogInitializer.getLogFilesInitializer(),
                new IndexImporterFactoryImpl(),
                EmptyMemoryTracker.INSTANCE,
                CursorContextFactory.NULL_CONTEXT_FACTORY
            );
        return new BatchImporterAdapter(importer);
    }

    static final class ConfigurationAdapter implements org.neo4j.internal.batchimport.Configuration {
        private final org.neo4j.gds.compat.batchimport.Config inner;

        ConfigurationAdapter(org.neo4j.gds.compat.batchimport.Config inner) {
            this.inner = inner;
        }

        @Override
        public int batchSize() {
            return this.inner.batchSize();
        }

        @Override
        public int maxNumberOfWorkerThreads() {
            return this.inner.writeConcurrency();
        }

        @Override
        public boolean highIO() {
            return this.inner.highIO();
        }

        @Override
        public IndexConfig indexConfig() {
            var config = IndexConfig.DEFAULT;
            if (this.inner.createLabelIndex()) {
                config = config.withLabelIndex();
            }
            if (this.inner.createRelationshipTypeIndex()) {
                config = config.withRelationshipTypeIndex();
            }
            return config;
        }
    }

    @Override
    public ExecutionMonitor newCoarseBoundedProgressExecutionMonitor(
        long highNodeId,
        long highRelationshipId,
        int batchSize,
        LongConsumer progress,
        LongConsumer outNumberOfBatches
    ) {
        var delegate = new CoarseBoundedProgressExecutionMonitor(
            highNodeId,
            highRelationshipId,
            Configuration.withBatchSize(Configuration.DEFAULT, batchSize)
        ) {
            @Override
            protected void progress(long l) {
                progress.accept(l);
            }

            long numberOfBatches() {
                return this.total();
            }
        };
        // Note: this only works because we declare the delegate with `var`
        outNumberOfBatches.accept(delegate.numberOfBatches());

        return new ExecutionMonitor() {

            @Override
            public org.neo4j.gds.compat.batchimport.Monitor toMonitor() {
                throw new UnsupportedOperationException("Cannot call  `toMonitor` on this one");
            }

            @Override
            public void start(StageExecution execution) {
                delegate.start(execution);
            }

            @Override
            public void end(StageExecution execution, long totalTimeMillis) {
                delegate.end(execution, totalTimeMillis);
            }

            @Override
            public void done(boolean successful, long totalTimeMillis, String additionalInformation) {
                delegate.done(successful, totalTimeMillis, additionalInformation);
            }

            @Override
            public long checkIntervalMillis() {
                return delegate.checkIntervalMillis();
            }

            @Override
            public void check(StageExecution execution) {
                delegate.check(execution);
            }
        };
    }

    static final class ExecutionMonitorAdapter implements org.neo4j.internal.batchimport.staging.ExecutionMonitor {
        private final ExecutionMonitor delegate;

        ExecutionMonitorAdapter(ExecutionMonitor delegate) {this.delegate = delegate;}

        @Override
        public void initialize(DependencyResolver dependencyResolver) {
            org.neo4j.internal.batchimport.staging.ExecutionMonitor.super.initialize(dependencyResolver);
            this.delegate.initialize(dependencyResolver);
        }

        @Override
        public void start(StageExecution stageExecution) {
            this.delegate.start(stageExecution);
        }

        @Override
        public void end(StageExecution stageExecution, long l) {
            this.delegate.end(stageExecution, l);
        }

        @Override
        public void done(boolean b, long l, String s) {
            this.delegate.done(b, l, s);
        }

        @Override
        public long checkIntervalMillis() {
            return this.delegate.checkIntervalMillis();
        }

        @Override
        public void check(StageExecution stageExecution) {
            this.delegate.check(stageExecution);
        }
    }

    static final class BatchImporterAdapter implements BatchImporter {
        private final org.neo4j.internal.batchimport.BatchImporter delegate;

        BatchImporterAdapter(org.neo4j.internal.batchimport.BatchImporter delegate) {this.delegate = delegate;}

        @Override
        public void doImport(Input input) throws IOException {
            throw new UnsupportedOperationException(
                "`org.neo4j.gds.compat._516.Neo4jProxyImpl.BatchImporterAdapter.doImport` is not yet implemented.");
        }
    }

    static final class InputAdapter implements org.neo4j.internal.batchimport.input.Input {
        private final Input delegate;

        InputAdapter(Input delegate) {this.delegate = delegate;}

        @Override
        public InputIterable nodes(org.neo4j.internal.batchimport.input.Collector collector) {
            delegate.nodes();
            throw new UnsupportedOperationException(
                "`org.neo4j.gds.compat._516.Neo4jProxyImpl.InputAdapter.nodes` is not yet implemented.");
        }

        @Override
        public InputIterable relationships(org.neo4j.internal.batchimport.input.Collector collector) {
            throw new UnsupportedOperationException(
                "`org.neo4j.gds.compat._516.Neo4jProxyImpl.InputAdapter.relationships` is not yet implemented.");
        }

        @Override
        public IdType idType() {
            throw new UnsupportedOperationException(
                "`org.neo4j.gds.compat._516.Neo4jProxyImpl.InputAdapter.idType` is not yet implemented.");
        }

        @Override
        public org.neo4j.internal.batchimport.input.ReadableGroups groups() {
            throw new UnsupportedOperationException(
                "`org.neo4j.gds.compat._516.Neo4jProxyImpl.InputAdapter.groups` is not yet implemented.");
        }

        @Override
        public Estimates calculateEstimates(PropertySizeCalculator propertySizeCalculator) throws IOException {
            throw new UnsupportedOperationException(
                "`org.neo4j.gds.compat._516.Neo4jProxyImpl.InputAdapter.calculateEstimates` is not yet implemented.");
        }

        @Override
        public Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
            return org.neo4j.internal.batchimport.input.Input.super.referencedNodeSchema(tokenHolders);
        }

        @Override
        public void close() {
            org.neo4j.internal.batchimport.input.Input.super.close();
        }
    }

    @Override
    public ReadableGroups newGroups() {
        // TODO: new Groups()
        throw new UnsupportedOperationException(
            "`org.neo4j.gds.compat._516.Neo4jProxyImpl.newGroups` is not yet implemented.");
    }

    @Override
    public Collector emptyCollector() {
        return CollectorAdapter.EMPTY;
    }

    @Override
    public Collector badCollector(OutputStream outputStream, int batchSize) {
        return new CollectorAdapter(Collectors.badCollector(outputStream, 0));
    }

    private static final class CollectorAdapter implements Collector {
        private static final Collector EMPTY = new CollectorAdapter(org.neo4j.internal.batchimport.input.Collector.EMPTY);

        private final org.neo4j.internal.batchimport.input.Collector inner;

        CollectorAdapter(org.neo4j.internal.batchimport.input.Collector inner) {
            this.inner = inner;
        }
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
        org.neo4j.internal.kernel.api.Write neoWrite = kernelTransaction.dataWrite();
        return new Write() {

            @Override
            public void nodeAddLabel(long node, int nodeLabelToken) throws KernelException {
                neoWrite.nodeAddLabel(node, nodeLabelToken);
            }

            @Override
            public long relationshipCreate(long source, int relationshipToken, long target) throws KernelException {
                return neoWrite.relationshipCreate(source, relationshipToken, target);
            }

            @Override
            public void nodeSetProperty(long node, int propertyKey, Value value) throws KernelException {
                neoWrite.nodeSetProperty(node, propertyKey, value);
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
        String category = null;      // No predefined categpry (like temporal or math)
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
}
