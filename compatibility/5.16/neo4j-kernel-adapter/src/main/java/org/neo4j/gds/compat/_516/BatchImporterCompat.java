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
import org.neo4j.gds.compat.batchimport.BatchImporter;
import org.neo4j.gds.compat.batchimport.ExecutionMonitor;
import org.neo4j.gds.compat.batchimport.ImportConfig;
import org.neo4j.gds.compat.batchimport.InputIterable;
import org.neo4j.gds.compat.batchimport.InputIterator;
import org.neo4j.gds.compat.batchimport.Monitor;
import org.neo4j.gds.compat.batchimport.input.Collector;
import org.neo4j.gds.compat.batchimport.input.Group;
import org.neo4j.gds.compat.batchimport.input.IdType;
import org.neo4j.gds.compat.batchimport.input.Input;
import org.neo4j.gds.compat.batchimport.input.InputChunk;
import org.neo4j.gds.compat.batchimport.input.InputEntityVisitor;
import org.neo4j.gds.compat.batchimport.input.PropertySizeCalculator;
import org.neo4j.gds.compat.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.IndexConfig;
import org.neo4j.internal.batchimport.input.Collectors;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.internal.batchimport.staging.StageExecution;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.LongConsumer;

public final class BatchImporterCompat {

    private BatchImporterCompat() {}

    static BatchImporter instantiateRecordBatchImporter(
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        ImportConfig config,
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
                org.neo4j.internal.batchimport.Monitor.NO_MONITOR,
                jobScheduler,
                badCollector != null ? ((CollectorAdapter) badCollector).inner : null,
                TransactionLogInitializer.getLogFilesInitializer(),
                new IndexImporterFactoryImpl(),
                EmptyMemoryTracker.INSTANCE,
                CursorContextFactory.NULL_CONTEXT_FACTORY
            );
        return new BatchImporterReverseAdapter(importer);
    }

    static final class ConfigurationAdapter implements org.neo4j.internal.batchimport.Configuration {
        private final ImportConfig inner;

        ConfigurationAdapter(ImportConfig inner) {
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

    static ExecutionMonitor newCoarseBoundedProgressExecutionMonitor(
        long highNodeId,
        long highRelationshipId,
        int batchSize,
        LongConsumer progress,
        LongConsumer outNumberOfBatches
    ) {
        var delegate = new CoarseBoundedProgressExecutionMonitor(
            highNodeId,
            highRelationshipId,
            org.neo4j.internal.batchimport.Configuration.withBatchSize(
                org.neo4j.internal.batchimport.Configuration.DEFAULT,
                batchSize
            )
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
            public Monitor toMonitor() {
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

    static final class BatchImporterReverseAdapter implements BatchImporter {
        private final org.neo4j.internal.batchimport.BatchImporter delegate;

        BatchImporterReverseAdapter(org.neo4j.internal.batchimport.BatchImporter delegate) {this.delegate = delegate;}

        @Override
        public void doImport(Input input) throws IOException {
            delegate.doImport(new InputAdapter(input));
        }
    }

    static final class InputAdapter implements org.neo4j.internal.batchimport.input.Input {
        private final Input delegate;

        InputAdapter(Input delegate) {this.delegate = delegate;}

        @Override
        public org.neo4j.internal.batchimport.InputIterable nodes(org.neo4j.internal.batchimport.input.Collector collector) {
            return new InputIterableAdapter(delegate.nodes());
        }

        @Override
        public org.neo4j.internal.batchimport.InputIterable relationships(org.neo4j.internal.batchimport.input.Collector collector) {
            return new InputIterableAdapter(delegate.relationships());
        }

        @Override
        public org.neo4j.internal.batchimport.input.IdType idType() {
            return adaptIdType(delegate.idType());
        }

        @Override
        public org.neo4j.internal.batchimport.input.ReadableGroups groups() {
            return adaptReadableGroups(delegate.groups());
        }

        @Override
        public Estimates calculateEstimates(org.neo4j.internal.batchimport.input.PropertySizeCalculator propertySizeCalculator) throws
            IOException {
            return adaptEstimates(delegate.calculateEstimates(
                new PropertySizeCalculatorReverseAdapter(propertySizeCalculator)
            ));
        }

        @Override
        public Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
            return delegate.referencedNodeSchema(tokenHolders);
        }

        @Override
        public void close() {
            org.neo4j.internal.batchimport.input.Input.super.close();
            delegate.close();
        }
    }

    static final class InputIterableAdapter implements org.neo4j.internal.batchimport.InputIterable {
        private final InputIterable delegate;

        InputIterableAdapter(InputIterable delegate) {this.delegate = delegate;}

        @Override
        public org.neo4j.internal.batchimport.InputIterator iterator() {
            return new InputIteratorAdapter(delegate.iterator());
        }
    }

    static final class InputIteratorAdapter implements org.neo4j.internal.batchimport.InputIterator {
        private final InputIterator delegate;

        InputIteratorAdapter(InputIterator delegate) {this.delegate = delegate;}

        @Override
        public org.neo4j.internal.batchimport.input.InputChunk newChunk() {
            return new InputChunkAdapter(delegate.newChunk());
        }

        @Override
        public boolean next(org.neo4j.internal.batchimport.input.InputChunk inputChunk) throws IOException {
            return inputChunk instanceof InputChunkAdapter adapter
                ? delegate.next(adapter.delegate)
                : delegate.next(new InputChunkReverseAdapter(inputChunk));
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class InputChunkAdapter implements org.neo4j.internal.batchimport.input.InputChunk {
        private final InputChunk delegate;

        private InputChunkAdapter(InputChunk delegate) {this.delegate = delegate;}

        @Override
        public boolean next(org.neo4j.internal.batchimport.input.InputEntityVisitor inputEntityVisitor) throws
            IOException {
            return delegate.next(new InputEntityVisitorReverseAdapter(inputEntityVisitor));
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class InputChunkReverseAdapter implements InputChunk {
        private final org.neo4j.internal.batchimport.input.InputChunk delegate;

        InputChunkReverseAdapter(org.neo4j.internal.batchimport.input.InputChunk delegate) {this.delegate = delegate;}

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            return delegate.next(new InputEntityVisitorAdapter(visitor));
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class InputEntityVisitorAdapter implements org.neo4j.internal.batchimport.input.InputEntityVisitor {
        private final InputEntityVisitor delegate;

        InputEntityVisitorAdapter(InputEntityVisitor delegate) {this.delegate = delegate;}

        @Override
        public boolean propertyId(long l) {
            return delegate.propertyId(l);
        }

        @Override
        public boolean property(String s, Object o) {
            return delegate.property(s, o);
        }

        @Override
        public boolean property(int i, Object o) {
            return delegate.property(i, o);
        }

        @Override
        public boolean id(long l) {
            return delegate.id(l);
        }

        @Override
        public boolean id(Object o, org.neo4j.internal.batchimport.input.Group group) {
            return delegate.id(o, adaptGroup(group));
        }

        @Override
        public boolean id(Object o, org.neo4j.internal.batchimport.input.Group group, IdSequence idSequence) {
            return delegate.id(o, adaptGroup(group), idSequence);
        }

        @Override
        public boolean labels(String[] strings) {
            return delegate.labels(strings);
        }

        @Override
        public boolean labelField(long l) {
            return delegate.labelField(l);
        }

        @Override
        public boolean startId(long l) {
            return delegate.startId(l);
        }

        @Override
        public boolean startId(Object o, org.neo4j.internal.batchimport.input.Group group) {
            return delegate.startId(o, adaptGroup(group));
        }

        @Override
        public boolean endId(long l) {
            return delegate.endId(l);
        }

        @Override
        public boolean endId(Object o, org.neo4j.internal.batchimport.input.Group group) {
            return delegate.endId(o, adaptGroup(group));
        }

        @Override
        public boolean type(int i) {
            return delegate.type(i);
        }

        @Override
        public boolean type(String s) {
            return delegate.type(s);
        }

        @Override
        public void endOfEntity() throws IOException {
            delegate.endOfEntity();
        }

        @Override
        public void reset() {
            delegate.reset();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class InputEntityVisitorReverseAdapter implements InputEntityVisitor {
        private final org.neo4j.internal.batchimport.input.InputEntityVisitor delegate;

        InputEntityVisitorReverseAdapter(org.neo4j.internal.batchimport.input.InputEntityVisitor delegate) {this.delegate = delegate;}

        @Override
        public boolean propertyId(long l) {
            return delegate.propertyId(l);
        }

        @Override
        public boolean properties(ByteBuffer properties, boolean offloaded) {
            throw new UnsupportedOperationException("Method is not available on Neo4j 5.17");
        }

        @Override
        public boolean property(String s, Object o) {
            return delegate.property(s, o);
        }

        @Override
        public boolean property(int i, Object o) {
            return delegate.property(i, o);
        }

        @Override
        public boolean id(long l) {
            return delegate.id(l);
        }

        @Override
        public boolean id(Object id, Group group) {
            return delegate.id(id, adaptGroup(group));
        }

        @Override
        public boolean id(Object id, Group group, IdSequence idSequence) {
            return delegate.id(id, adaptGroup(group), idSequence);
        }

        @Override
        public boolean labels(String[] strings) {
            return delegate.labels(strings);
        }

        @Override
        public boolean labelField(long l) {
            return delegate.labelField(l);
        }

        @Override
        public boolean startId(long l) {
            return delegate.startId(l);
        }

        @Override
        public boolean startId(Object id, Group group) {
            return delegate.startId(id, adaptGroup(group));
        }

        @Override
        public boolean endId(long l) {
            return delegate.endId(l);
        }

        @Override
        public boolean endId(Object id, Group group) {
            return delegate.endId(id, adaptGroup(group));
        }

        @Override
        public boolean type(int i) {
            return delegate.type(i);
        }

        @Override
        public boolean type(String s) {
            return delegate.type(s);
        }

        @Override
        public void endOfEntity() throws IOException {
            delegate.endOfEntity();
        }

        @Override
        public void reset() {
            delegate.reset();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static org.neo4j.internal.batchimport.input.Group adaptGroup(Group group) {
        return ((GroupReverseAdapter) group).delegate;
    }

    private static Group adaptGroup(org.neo4j.internal.batchimport.input.Group group) {
        return new GroupReverseAdapter(group);
    }

    private static final class GroupReverseAdapter implements Group {
        private final org.neo4j.internal.batchimport.input.Group delegate;

        private GroupReverseAdapter(org.neo4j.internal.batchimport.input.Group delegate) {this.delegate = delegate;}
    }

    private static org.neo4j.internal.batchimport.input.IdType adaptIdType(IdType idType) {
        return switch (idType) {
            case ACTUAL -> org.neo4j.internal.batchimport.input.IdType.ACTUAL;
            case INTEGER -> org.neo4j.internal.batchimport.input.IdType.INTEGER;
            case STRING -> org.neo4j.internal.batchimport.input.IdType.STRING;
        };
    }

    private static org.neo4j.internal.batchimport.input.ReadableGroups adaptReadableGroups(ReadableGroups groups) {
        return groups == null || groups == ReadableGroups.EMPTY
            ? org.neo4j.internal.batchimport.input.ReadableGroups.EMPTY
            : ((ReadableGroupsReverseAdapter) groups).delegate;
    }

    private static ReadableGroups adaptReadableGroups(org.neo4j.internal.batchimport.input.ReadableGroups groups) {
        return groups == null || groups == org.neo4j.internal.batchimport.input.ReadableGroups.EMPTY
            ? ReadableGroups.EMPTY
            : new ReadableGroupsReverseAdapter(groups);
    }

    private static class ReadableGroupsReverseAdapter implements ReadableGroups {
        private final org.neo4j.internal.batchimport.input.ReadableGroups delegate;

        ReadableGroupsReverseAdapter(org.neo4j.internal.batchimport.input.ReadableGroups delegate) {this.delegate = delegate;}

        @Override
        public Group getGlobalGroup() {
            return adaptGroup(delegate.get(null));
        }
    }

    private static org.neo4j.internal.batchimport.input.Input.Estimates adaptEstimates(Input.Estimates estimates) {
        return new org.neo4j.internal.batchimport.input.Input.Estimates(
            estimates.numberOfNodes(),
            estimates.numberOfRelationships(),
            estimates.numberOfNodeProperties(),
            estimates.numberOfRelationshipProperties(),
            estimates.sizeOfNodeProperties(),
            estimates.sizeOfRelationshipProperties(),
            estimates.numberOfNodeLabels()
        );
    }

    private static final class PropertySizeCalculatorReverseAdapter implements PropertySizeCalculator {
        private final org.neo4j.internal.batchimport.input.PropertySizeCalculator delegate;

        PropertySizeCalculatorReverseAdapter(org.neo4j.internal.batchimport.input.PropertySizeCalculator delegate) {this.delegate = delegate;}

        @Override
        public int calculateSize(Value[] values, CursorContext cursorContext, MemoryTracker memoryTracker) {
            return delegate.calculateSize(values, cursorContext, memoryTracker);
        }
    }

    static ReadableGroups newGroups() {
        return adaptReadableGroups(new Groups());
    }

    static ReadableGroups newInitializedGroups() {
        var groups = new Groups();
        groups.getOrCreate(null);
        return adaptReadableGroups(groups);
    }

    static Collector emptyCollector() {
        return CollectorAdapter.EMPTY;
    }

    static Collector badCollector(OutputStream outputStream, int batchSize) {
        return new CollectorAdapter(Collectors.badCollector(outputStream, batchSize));
    }

    private static final class CollectorAdapter implements Collector {
        private static final Collector EMPTY = new CollectorAdapter(org.neo4j.internal.batchimport.input.Collector.EMPTY);

        private final org.neo4j.internal.batchimport.input.Collector inner;

        CollectorAdapter(org.neo4j.internal.batchimport.input.Collector inner) {
            this.inner = inner;
        }
    }
}
