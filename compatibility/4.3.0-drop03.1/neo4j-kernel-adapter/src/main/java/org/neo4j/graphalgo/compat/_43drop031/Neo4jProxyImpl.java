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
package org.neo4j.graphalgo.compat._43drop031;

import org.apache.commons.io.output.WriterOutputStream;
import org.eclipse.collections.api.factory.Sets;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphalgo.compat.CompatIndexQuery;
import org.neo4j.graphalgo.compat.CompatInput;
import org.neo4j.graphalgo.compat.CompositeNodeCursor;
import org.neo4j.graphalgo.compat.CustomAccessMode;
import org.neo4j.graphalgo.compat.ScanBasedStoreScan;
import org.neo4j.graphalgo.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphalgo.compat.JobRunner;
import org.neo4j.graphalgo.compat.MemoryTrackerProxy;
import org.neo4j.graphalgo.compat.Neo4jProxyApi;
import org.neo4j.graphalgo.compat.StoreScan;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.cache.LongArray;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.cache.OffHeapLongArray;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.PropertySizeCalculator;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.procedure.Mode;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.storable.ValueGroup;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class Neo4jProxyImpl implements Neo4jProxyApi {

    @Override
    public GdsGraphDatabaseAPI newDb(DatabaseManagementService dbms) {
        return new CompatGraphDatabaseAPIImpl(dbms);
    }

    @Override
    public AccessMode accessMode(CustomAccessMode customAccessMode) {
        return new CompatAccessModeImpl(customAccessMode);
    }

    @Override
    public AccessMode newRestrictedAccessMode(
        AccessMode original,
        AccessMode.Static restricting
    ) {
        return new RestrictedAccessMode(original, restricting);
    }

    @Override
    public SecurityContext securityContext(
        String username,
        AuthSubject authSubject,
        AccessMode mode,
        String databaseName
    ) {
        return new SecurityContext(new CompatUsernameAuthSubjectImpl(username, authSubject), mode);
    }

    @Override
    public long getHighestPossibleIdInUse(
        RecordStore<? extends AbstractBaseRecord> recordStore,
        KernelTransaction kernelTransaction
    ) {
        return recordStore.getHighestPossibleIdInUse(kernelTransaction.pageCursorTracer());
    }

    @Override
    public PageCursor pageFileIO(PagedFile pagedFile, long pageId, int pageFileFlags) throws IOException {
        return pagedFile.io(pageId, pageFileFlags, PageCursorTracer.NULL);
    }

    @Override
    public PagedFile pageCacheMap(
        PageCache pageCache,
        File file,
        int pageSize,
        String databaseName,
        OpenOption... openOptions
    ) throws IOException {
        return pageCache.map(file.toPath(), pageSize, databaseName, Sets.immutable.of(openOptions));
    }

    @Override
    public Path pagedFile(PagedFile pagedFile) {
        return pagedFile.path();
    }

    @Override
    public List<Scan<NodeLabelIndexCursor>> entityCursorScan(KernelTransaction transaction, int[] labelIds) {
        var read = transaction.dataRead();
        read.prepareForLabelScans();
        return Arrays
            .stream(labelIds)
            .mapToObj(read::nodeLabelScan)
            .collect(Collectors.toList());
    }

    @Override
    public PropertyCursor allocatePropertyCursor(KernelTransaction kernelTransaction) {
        return kernelTransaction
            .cursors()
            .allocatePropertyCursor(kernelTransaction.pageCursorTracer(), kernelTransaction.memoryTracker());
    }

    @Override
    public NodeCursor allocateNodeCursor(KernelTransaction kernelTransaction) {
        return kernelTransaction.cursors().allocateNodeCursor(kernelTransaction.pageCursorTracer());
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor(KernelTransaction kernelTransaction) {
        return kernelTransaction.cursors().allocateRelationshipScanCursor(kernelTransaction.pageCursorTracer());
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor(KernelTransaction kernelTransaction) {
        return kernelTransaction.cursors().allocateNodeLabelIndexCursor(kernelTransaction.pageCursorTracer());
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor(KernelTransaction kernelTransaction) {
        return kernelTransaction
            .cursors()
            .allocateNodeValueIndexCursor(kernelTransaction.pageCursorTracer(), kernelTransaction.memoryTracker());
    }

    @Override
    public long relationshipsReference(NodeCursor nodeCursor) {
        return nodeCursor.relationshipsReference();
    }

    @Override
    public boolean hasNodeLabelIndex(KernelTransaction kernelTransaction) {
        return NodeLabelIndexLookupImpl.hasNodeLabelIndex(kernelTransaction);
    }

    @Override
    public void nodeLabelScan(KernelTransaction kernelTransaction, int label, NodeLabelIndexCursor cursor) {
        kernelTransaction.dataRead().nodeLabelScan(label, cursor, IndexOrder.NONE);
    }

    @Override
    public StoreScan<NodeLabelIndexCursor> nodeLabelIndexScan(
        KernelTransaction transaction,
        int labelId,
        int batchSize
    ) {
        var read = transaction.dataRead();
        read.prepareForLabelScans();
        return new ScanBasedStoreScan<>(read.nodeLabelScan(labelId), batchSize);
    }

    @Override
    public void nodeIndexScan(
        Read dataRead, IndexReadSession index, NodeValueIndexCursor cursor, IndexOrder indexOrder, boolean needsValues
    ) throws Exception {
        var indexQueryConstraints = indexOrder == IndexOrder.NONE
            ? IndexQueryConstraints.unordered(needsValues)
            : IndexQueryConstraints.constrained(indexOrder, needsValues);

        dataRead.nodeIndexScan(index, cursor, indexQueryConstraints);
    }

    @Override
    public CompatIndexQuery rangeIndexQuery(
        int propertyKeyId,
        double from,
        boolean fromInclusive,
        double to,
        boolean toInclusive
    ) {
        return new CompatIndexQueryImpl(PropertyIndexQuery.range(propertyKeyId, from, fromInclusive, to, toInclusive));
    }

    @Override
    public CompatIndexQuery rangeAllIndexQuery(int propertyKeyId) {
        return new CompatIndexQueryImpl(PropertyIndexQuery.range(propertyKeyId, ValueGroup.NUMBER));
    }

    @Override
    public void nodeIndexSeek(
        Read dataRead,
        IndexReadSession index,
        NodeValueIndexCursor cursor,
        IndexOrder indexOrder,
        boolean needsValues,
        CompatIndexQuery query
    ) throws Exception {
        var indexQueryConstraints = indexOrder == IndexOrder.NONE
            ? IndexQueryConstraints.unordered(needsValues)
            : IndexQueryConstraints.constrained(indexOrder, needsValues);

        dataRead.nodeIndexSeek(index, cursor, indexQueryConstraints, ((CompatIndexQueryImpl) query).indexQuery);
    }

    @Override
    public CompositeNodeCursor compositeNodeCursor(List<NodeLabelIndexCursor> cursors, int[] labelIds) {
        return new CompositeNodeCursorImpl(cursors, labelIds);
    }

    @Override
    public OffHeapLongArray newOffHeapLongArray(long length, long defaultValue, long base) {
        return new OffHeapLongArray(length, defaultValue, base, EmptyMemoryTracker.INSTANCE);
    }

    @Override
    public LongArray newChunkedLongArray(NumberArrayFactory numberArrayFactory, int size, long defaultValue) {
        return numberArrayFactory.newLongArray(size, defaultValue, EmptyMemoryTracker.INSTANCE);
    }

    @Override
    public MemoryTrackerProxy memoryTrackerProxy(KernelTransaction kernelTransaction) {
        return MemoryTrackerProxyImpl.of(kernelTransaction.memoryTracker());
    }

    @Override
    public MemoryTrackerProxy emptyMemoryTracker() {
        return MemoryTrackerProxyImpl.of(EmptyMemoryTracker.INSTANCE);
    }

    @Override
    public MemoryTrackerProxy limitedMemoryTracker(long limitInBytes, long grabSizeInBytes) {
        return MemoryTrackerProxyImpl.of(new LocalMemoryTracker(
            MemoryPools.NO_TRACKING,
            limitInBytes,
            grabSizeInBytes,
            "setting"
        ));
    }

    @Override
    public LogService logProviderForStoreAndRegister(
        Path storeLogPath,
        FileSystemAbstraction fs,
        LifeSupport lifeSupport
    ) {
        var neo4jLoggerContext = LogConfig.createBuilder(fs, storeLogPath, Level.INFO).build();
        var simpleLogService = new SimpleLogService(
            NullLogProvider.getInstance(),
            new Log4jLogProvider(neo4jLoggerContext)
        );
        return lifeSupport.add(simpleLogService);
    }

    @Override
    public Path metadataStore(DatabaseLayout databaseLayout) {
        return databaseLayout.metadataStore();
    }

    @Override
    public Path homeDirectory(DatabaseLayout databaseLayout) {
        return databaseLayout.getNeo4jLayout().homeDirectory();
    }

    @Override
    public BatchImporter instantiateBatchImporter(
        BatchImporterFactory factory,
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        PageCacheTracer pageCacheTracer,
        int writeConcurrency,
        Optional<Long> pageCacheMemory,
        LogService logService,
        ExecutionMonitor executionMonitor,
        AdditionalInitialIds additionalInitialIds,
        Config dbConfig,
        RecordFormats recordFormats,
        ImportLogic.Monitor monitor,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        var importerConfig = new Configuration() {
            @Override
            public int maxNumberOfProcessors() {
                return writeConcurrency;
            }

            @Override
            public long pageCacheMemory() {
                return pageCacheMemory.orElseGet(() -> Configuration.super.pageCacheMemory());
            }

            @Override
            public boolean highIO() {
                return false;
            }
        };
        return factory.instantiate(
            directoryStructure,
            fileSystem,
            pageCacheTracer,
            importerConfig,
            logService,
            executionMonitor,
            additionalInitialIds,
            dbConfig,
            recordFormats,
            monitor,
            jobScheduler,
            badCollector,
            TransactionLogInitializer.getLogFilesInitializer(),
            EmptyMemoryTracker.INSTANCE
        );
    }

    @Override
    public Input batchInputFrom(CompatInput compatInput) {
        return new InputFromCompatInput(compatInput);
    }

    @Override
    public String queryText(ExecutingQuery query) {
        return query.rawQueryText();
    }

    @Override
    public Log logger(
        Level level,
        ZoneId zoneId,
        DateTimeFormatter dateTimeFormatter,
        String category,
        PrintWriter writer
    ) {
        var outStream = new WriterOutputStream(writer, StandardCharsets.UTF_8);
        return this.logger(level, zoneId, dateTimeFormatter, category, outStream);
    }

    @Override
    public Log logger(
        Level level,
        ZoneId zoneId,
        DateTimeFormatter dateTimeFormatter,
        String category,
        OutputStream outputStream
    ) {
        var logTimeZone = Arrays
            .stream(LogTimeZone.values())
            .filter(tz -> tz.getZoneId().equals(zoneId))
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException("Can only log in UTC or " + LogTimeZone.SYSTEM.getZoneId()));
        var context = LogConfig
            .createBuilder(outputStream, level)
            .withCategory(category != null)
            .withTimezone(logTimeZone)
            .build();

        return new Log4jLogProvider(context).getLog(category != null ? category : "");
    }

    @Override
    public Setting<Boolean> onlineBackupEnabled() {
        try {
            Class<?> onlineSettingsClass = Class.forName(
                "com.neo4j.configuration.OnlineBackupSettings");
            var onlineBackupEnabled = MethodHandles
                .lookup()
                .findStaticGetter(onlineSettingsClass, "online_backup_enabled", Setting.class)
                .invoke();
            //noinspection unchecked
            return (Setting<Boolean>) onlineBackupEnabled;
        } catch (Throwable e) {
            throw new IllegalStateException(
                "The online_backup_enabled setting requires Neo4j Enterprise Edition to be available.");
        }
    }

    @Override
    public Setting<String> additionalJvm() {
        return BootloaderSettings.additional_jvm;
    }

    @Override
    public Setting<Long> memoryTransactionMaxSize() {
        return GraphDatabaseSettings.memory_transaction_max_size;
    }

    @Override
    public JobRunner runnerFromScheduler(JobScheduler scheduler, Group group) {
        return new JobRunnerImpl(scheduler, group);
    }

    @Override
    public ExecutionMonitor invisibleExecutionMonitor() {
        return ExecutionMonitor.INVISIBLE;
    }

    @Override
    public UserFunctionSignature userFunctionSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        Neo4jTypes.AnyType type,
        String deprecated,
        String[] allowed,
        String description,
        String category,
        boolean caseInsensitive
    ) {
        return new UserFunctionSignature(
            name,
            inputSignature,
            type,
            deprecated,
            allowed,
            description,
            category,
            caseInsensitive
        );
    }

    @Override
    public ProcedureSignature procedureSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        List<FieldSignature> outputSignature,
        Mode mode,
        boolean admin,
        String deprecated,
        String[] allowed,
        String description,
        String warning,
        boolean eager,
        boolean caseInsensitive,
        boolean systemProcedure,
        boolean internal,
        boolean allowExpiredCredentials
    ) {
        return new ProcedureSignature(
            name,
            inputSignature,
            outputSignature,
            mode,
            admin,
            deprecated,
            allowed,
            description,
            warning,
            eager,
            caseInsensitive,
            systemProcedure,
            internal,
            allowExpiredCredentials
        );
    }

    @Override
    public <T> ThrowingFunction<Context, T, ProcedureException> lookupComponentProvider(
        GlobalProcedures registry, Class<T> cls, boolean safe
    ) {
        return registry.lookupComponentProvider(cls, safe);
    }

    private static final class InputFromCompatInput implements Input {
        private final CompatInput delegate;

        private InputFromCompatInput(CompatInput delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputIterable nodes(Collector badCollector) {
            return delegate.nodes(badCollector);
        }

        @Override
        public InputIterable relationships(Collector badCollector) {
            return delegate.relationships(badCollector);
        }

        @Override
        public IdType idType() {
            return delegate.idType();
        }

        @Override
        public ReadableGroups groups() {
            return delegate.groups();
        }

        @Override
        public Estimates calculateEstimates(PropertySizeCalculator propertySizeCalculator) throws IOException {
            return delegate.calculateEstimates((values, kernelTransaction) -> propertySizeCalculator.calculateSize(
                values,
                kernelTransaction.pageCursorTracer(),
                kernelTransaction.memoryTracker()
            ));
        }
    }
}
