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

import org.eclipse.collections.api.factory.Sets;
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
import org.neo4j.internal.batchimport.input.PropertySizeCalculator;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.newapi.OpenCursorFactory41;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.OpenOption;

public final class Neo4jProxy41 implements Neo4jProxyApi {

    @Override
    public GdsGraphDatabaseAPI newDb(DatabaseManagementService dbms) {
        return new CompatGraphDatabaseAPI41(dbms);
    }

    @Override
    public AccessMode accessMode(CustomAccessMode customAccessMode) {
        return new CompatAccessMode41(customAccessMode);
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
        recordFormat.read(record, cursor, mode, recordSize, recordsPerPage);
    }

    @Override
    public long getHighestPossibleIdInUse(
        RecordStore<? extends AbstractBaseRecord> recordStore,
        PageCursorTracer pageCursorTracer
    ) {
        return recordStore.getHighestPossibleIdInUse(pageCursorTracer);
    }

    @Override
    public <RECORD extends AbstractBaseRecord> PageCursor openPageCursorForReading(
        RecordStore<RECORD> recordStore,
        long pageId,
        PageCursorTracer pageCursorTracer
    ) {
        return recordStore.openPageCursorForReading(pageId, pageCursorTracer);
    }

    @Override
    public PageCursor pageFileIO(
        PagedFile pagedFile,
        long pageId,
        int pageFileFlags,
        PageCursorTracer pageCursorTracer
    ) throws IOException {
        return pagedFile.io(pageId, pageFileFlags, pageCursorTracer);
    }

    @Override
    public PagedFile pageCacheMap(
        PageCache pageCache,
        File file,
        int pageSize,
        OpenOption... openOptions
    ) throws IOException {
        return pageCache.map(file, pageSize, Sets.immutable.of(openOptions));
    }


    @Override
    public NodeCursor allocateNodeCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return cursorFactory.allocateNodeCursor(cursorTracer);
    }

    @Override
    public NodeCursor allocateFullAccessNodeCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return cursorFactory.allocateFullAccessNodeCursor(cursorTracer);
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return cursorFactory.allocateRelationshipScanCursor(cursorTracer);
    }

    @Override
    public RelationshipScanCursor allocateFullAccessRelationshipScanCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return cursorFactory.allocateFullAccessRelationshipScanCursor(cursorTracer);
    }

    @Override
    public PropertyCursor allocatePropertyCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer,
        MemoryTracker memoryTracker
    ) {
        return cursorFactory.allocatePropertyCursor(cursorTracer, memoryTracker);
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer,
        MemoryTracker memoryTracker
    ) {
        return cursorFactory.allocateFullAccessPropertyCursor(cursorTracer, memoryTracker);
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return cursorFactory.allocateNodeLabelIndexCursor(cursorTracer);
    }

    @Override
    public NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return OpenCursorFactory41.allocateFullAccessNodeLabelIndexCursor(cursorFactory, cursorTracer);
    }

    @Override
    public long[] getNodeLabelFields(NodeRecord node, NodeStore nodeStore, PageCursorTracer cursorTracer) {
        return NodeLabelsField.get(node, nodeStore, cursorTracer);
    }

    @Override
    public void nodeLabelScan(Read dataRead, int label, NodeLabelIndexCursor cursor) {
        dataRead.nodeLabelScan(label, cursor, IndexOrder.NONE);
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
    public MemoryTracker memoryTracker(KernelTransaction kernelTransaction) {
        return kernelTransaction.memoryTracker();
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
        return factory.instantiate(
            directoryStructure,
            fileSystem,
            externalPageCache,
            pageCacheTracer,
            config,
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
    public Log toPrintWriter(FormattedLog.Builder builder, PrintWriter writer) {
        return builder.toPrintWriter(() -> writer);
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
            throw new IllegalStateException("The online_backup_enabled setting requires Neo4j Enterprise Edition to be available.");
        }
    }

    @Override
    public Setting<String> additionalJvm() {
        return ExternalSettings.additional_jvm;
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
            return delegate.calculateEstimates(propertySizeCalculator::calculateSize);
        }
    }
}
