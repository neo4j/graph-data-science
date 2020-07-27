/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.compat;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ExternalSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
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
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.StoreLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.storable.Value;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.ToIntFunction;

public final class Neo4jProxy40 implements Neo4jProxyApi {

    @Override
    public GdsGraphDatabaseAPI newDb(DatabaseManagementService dbms) {
        return new CompatGraphDatabaseAPI40(dbms);
    }

    @Override
    public AccessMode accessMode(CustomAccessMode customAccessMode) {
        return new CompatAccessMode40(customAccessMode);
    }

    @Override
    public <RECORD extends AbstractBaseRecord> void read(
        RecordFormat<RECORD> recordFormat,
        RECORD record,
        PageCursor cursor,
        RecordLoad mode,
        int recordSize,
        int recordsPerPage
    ) throws IOException {
        recordFormat.read(record, cursor, mode, recordSize);
    }

    @Override
    public long getHighestPossibleIdInUse(
        RecordStore<? extends AbstractBaseRecord> recordStore,
        PageCursorTracer pageCursorTracer
    ) {
        return recordStore.getHighestPossibleIdInUse();
    }

    @Override
    public <RECORD extends AbstractBaseRecord> PageCursor openPageCursorForReading(
        RecordStore<RECORD> recordStore,
        long pageId,
        PageCursorTracer pageCursorTracer
    ) {
        return recordStore.openPageCursorForReading(pageId);
    }

    @Override
    public PageCursor pageFileIO(
        PagedFile pagedFile,
        long pageId,
        int pageFileFlags,
        PageCursorTracer pageCursorTracer
    ) throws IOException {
        return pagedFile.io(pageId, pageFileFlags);
    }

    @Override
    public PagedFile pageCacheMap(
        PageCache pageCache,
        File file,
        int pageSize,
        OpenOption... openOptions
    ) throws IOException {
        return pageCache.map(file, pageSize, openOptions);
    }

    @Override
    public PropertyCursor allocatePropertyCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer,
        MemoryTracker memoryTracker
    ) {
        return cursorFactory.allocatePropertyCursor();
    }

    @Override
    public NodeCursor allocateNodeCursor(CursorFactory cursorFactory, PageCursorTracer cursorTracer) {
        return cursorFactory.allocateNodeCursor();
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return cursorFactory.allocateRelationshipScanCursor();
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return cursorFactory.allocateNodeLabelIndexCursor();
    }

    @Override
    public long[] getNodeLabelFields(NodeRecord node, NodeStore nodeStore, PageCursorTracer cursorTracer) {
        return NodeLabelsField.get(node, nodeStore);
    }

    @Override
    public void nodeLabelScan(Read dataRead, int label, NodeLabelIndexCursor cursor) {
        dataRead.nodeLabelScan(label, cursor);
    }

    @Override
    public CompositeNodeCursor compositeNodeCursor(List<NodeLabelIndexCursor> cursors, int[] labelIds) {
        return new CompositeNodeCursor40(cursors, labelIds);
    }

    @Override
    public OffHeapLongArray newOffHeapLongArray(long length, long defaultValue, long base) {
        return new OffHeapLongArray(length, defaultValue, base);
    }

    @Override
    public LongArray newChunkedLongArray(NumberArrayFactory numberArrayFactory, int size, long defaultValue) {
        return numberArrayFactory.newLongArray(size, defaultValue);
    }

    @Override
    public MemoryTracker memoryTracker(KernelTransaction kernelTransaction) {
        return MemoryTracker.NONE;
    }

    @Override
    public LogService logProviderForStoreAndRegister(
        Path storeLogPath,
        FileSystemAbstraction fs,
        LifeSupport lifeSupport
    ) throws IOException {
        return lifeSupport.add(StoreLogService.withInternalLog(storeLogPath.toFile()).build(fs));
    }

    @Override
    public Path metadataStore(DatabaseLayout databaseLayout) {
        return databaseLayout.metadataStore().toPath();
    }

    @Override
    public BatchImporter instantiateBatchImporter(
        BatchImporterFactory factory,
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        PageCache externalPageCache,
        PageCacheTracer pageCacheTracer,
        Configuration config,
        LogService logService,
        ExecutionMonitor executionMonitor,
        AdditionalInitialIds additionalInitialIds,
        Config dbConfig,
        RecordFormats recordFormats,
        ImportLogic.Monitor monitor,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        return BatchImporterFactoryProxy.instantiateBatchImporter(
            factory,
            directoryStructure,
            fileSystem,
            externalPageCache,
            config,
            logService,
            executionMonitor,
            additionalInitialIds,
            dbConfig,
            recordFormats,
            monitor,
            jobScheduler,
            badCollector
        );
    }

    @Override
    public Input batchInputFrom(CompatInput compatInput) {
        return new InputFromCompatInput(compatInput);
    }

    @Override
    public String queryText(ExecutingQuery query) {
        return query.queryText();
    }

    @Override
    public Log logger(
        Level level,
        ZoneId zoneId,
        DateTimeFormatter dateTimeFormatter,
        String category,
        PrintWriter writer
    ) {
        return FormattedLog
            .withLogLevel(level)
            .withZoneId(zoneId)
            .withDateTimeFormatter(dateTimeFormatter)
            .withCategory(category)
            .toPrintWriter(() -> writer);
    }

    @Override
    public Log logger(
        Level level,
        ZoneId zoneId,
        DateTimeFormatter dateTimeFormatter,
        String category,
        OutputStream outputStream
    ) {
        return FormattedLog
            .withLogLevel(level)
            .withZoneId(zoneId)
            .withDateTimeFormatter(dateTimeFormatter)
            .withCategory(category)
            .toOutputStream(() -> outputStream);
    }

    @Override
    public Setting<Boolean> onlineBackupEnabled() {
        try {
            Class<?> onlineSettingsClass = Class.forName(
                "com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings");
            var onlineBackupEnabled = MethodHandles
                .lookup()
                .findStaticGetter(onlineSettingsClass, "online_backup_enabled", Setting.class)
                .invoke();
            //noinspection unchecked
            return (Setting<Boolean>) onlineBackupEnabled;
        } catch (Throwable e) {
            throw new IllegalStateException("The online_backup_enabled setting requires Neo4j Enterprise Edition to be available.");
        }
    }

    @Override
    public Setting<String> additionalJvm() {
        return ExternalSettings.additionalJvm;
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
        public Estimates calculateEstimates(ToIntFunction<Value[]> valueSizeCalculator) throws IOException {
            return delegate.calculateEstimates((values, cursorTracer, memoryTracker) ->
                valueSizeCalculator.applyAsInt(values));
        }
    }
}
