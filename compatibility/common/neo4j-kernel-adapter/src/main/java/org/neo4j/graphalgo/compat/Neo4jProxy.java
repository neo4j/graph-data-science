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
package org.neo4j.graphalgo.compat;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.cache.LongArray;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.cache.OffHeapLongArray;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
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
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.ServiceLoader;

public final class Neo4jProxy {

    private static final Neo4jProxyApi IMPL;

    static {
        var neo4jVersion = GraphDatabaseApiProxy.neo4jVersion();
        Neo4jProxyFactory neo4jProxyFactory = ServiceLoader
            .load(Neo4jProxyFactory.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(f -> f.canLoad(neo4jVersion))
            .findFirst()
            .orElseThrow(() -> new LinkageError("Could not load the " + Neo4jProxy.class + " implementation for " + neo4jVersion));
        IMPL = neo4jProxyFactory.load();
    }

    public static GdsGraphDatabaseAPI newDb(DatabaseManagementService dbms) {
        return IMPL.newDb(dbms);
    }

    public static AccessMode accessMode(CustomAccessMode customAccessMode) {
        return IMPL.accessMode(customAccessMode);
    }

    public static AccessMode newRestrictedAccessMode(
        AccessMode original,
        AccessMode.Static restricting
    ) {
        return IMPL.newRestrictedAccessMode(original, restricting);
    }

    public static AuthSubject usernameAuthSubject(String username, AuthSubject authSubject) {
        return IMPL.usernameAuthSubject(username, authSubject);
    }

    public static <RECORD extends AbstractBaseRecord> void read(
        RecordFormat<RECORD> recordFormat,
        RECORD record,
        PageCursor cursor,
        RecordLoad mode,
        int recordSize,
        int recordsPerPage
    ) throws IOException {
        IMPL.read(recordFormat, record, cursor, mode, recordSize, recordsPerPage);
    }

    public static long getHighestPossibleIdInUse(
        RecordStore<? extends AbstractBaseRecord> recordStore,
        PageCursorTracer pageCursorTracer
    ) {
        return IMPL.getHighestPossibleIdInUse(recordStore, pageCursorTracer);
    }

    public static <RECORD extends AbstractBaseRecord> PageCursor openPageCursorForReading(
        RecordStore<RECORD> recordStore,
        long pageId,
        PageCursorTracer pageCursorTracer
    ) {
        return IMPL.openPageCursorForReading(recordStore, pageId, pageCursorTracer);
    }

    public static PageCursor pageFileIO(
        PagedFile pagedFile,
        long pageId,
        int pageFileFlags,
        PageCursorTracer pageCursorTracer
    ) throws IOException {
        return IMPL.pageFileIO(pagedFile, pageId, pageFileFlags, pageCursorTracer);
    }

    public static PagedFile pageCacheMap(PageCache pageCache, File file, int pageSize, OpenOption... openOptions)
    throws IOException {
        return IMPL.pageCacheMap(pageCache, file, pageSize, openOptions);
    }

    public static Path pagedFile(PagedFile pagedFile) {
        return IMPL.pagedFile(pagedFile);
    }

    public static PropertyCursor allocatePropertyCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer,
        MemoryTracker memoryTracker
    ) {
        return IMPL.allocatePropertyCursor(cursorFactory, cursorTracer, memoryTracker);
    }

    public static NodeCursor allocateNodeCursor(CursorFactory cursorFactory, PageCursorTracer cursorTracer) {
        return IMPL.allocateNodeCursor(cursorFactory, cursorTracer);
    }

    public static RelationshipScanCursor allocateRelationshipScanCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return IMPL.allocateRelationshipScanCursor(cursorFactory, cursorTracer);
    }

    public static NodeLabelIndexCursor allocateNodeLabelIndexCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer
    ) {
        return IMPL.allocateNodeLabelIndexCursor(cursorFactory, cursorTracer);
    }

    public static NodeValueIndexCursor allocateNodeValueIndexCursor(
        CursorFactory cursorFactory,
        PageCursorTracer cursorTracer,
        MemoryTracker memoryTracker
    ) {
        return IMPL.allocateNodeValueIndexCursor(cursorFactory, cursorTracer, memoryTracker);
    }

    public static long relationshipsReference(NodeCursor nodeCursor) {
        return IMPL.relationshipsReference(nodeCursor);
    }

    public static long[] getNodeLabelFields(NodeRecord node, NodeStore nodeStore, PageCursorTracer cursorTracer) {
        return IMPL.getNodeLabelFields(node, nodeStore, cursorTracer);
    }

    public static void nodeLabelScan(Read dataRead, int label, NodeLabelIndexCursor cursor) {
        IMPL.nodeLabelScan(dataRead, label, cursor);
    }

    public static void nodeIndexScan(
        Read dataRead,
        IndexReadSession index,
        NodeValueIndexCursor cursor,
        IndexOrder indexOrder,
        boolean needsValues
    ) throws Exception {
        IMPL.nodeIndexScan(dataRead, index, cursor, indexOrder, needsValues);
    }

    public static CompatIndexQuery rangeIndexQuery(
        int propertyKeyId,
        double from,
        boolean fromInclusive,
        double to,
        boolean toInclusive
    ) {
        return IMPL.rangeIndexQuery(propertyKeyId, from, fromInclusive, to, toInclusive);
    }

    public static CompatIndexQuery rangeAllIndexQuery(int propertyKeyId) {
        return IMPL.rangeAllIndexQuery(propertyKeyId);
    }

    public static void nodeIndexSeek(
        Read dataRead,
        IndexReadSession index,
        NodeValueIndexCursor cursor,
        IndexOrder indexOrder,
        boolean needsValues,
        CompatIndexQuery query
    ) throws Exception {
        IMPL.nodeIndexSeek(dataRead, index, cursor, indexOrder, needsValues, query);
    }

    public static CompositeNodeCursor compositeNodeCursor(List<NodeLabelIndexCursor> cursors, int[] labelIds) {
        return IMPL.compositeNodeCursor(cursors, labelIds);
    }

    public static OffHeapLongArray newOffHeapLongArray(long length, long defaultValue, long base) {
        return IMPL.newOffHeapLongArray(length, defaultValue, base);
    }

    public static LongArray newChunkedLongArray(NumberArrayFactory numberArrayFactory, int size, long defaultValue) {
        return IMPL.newChunkedLongArray(numberArrayFactory, size, defaultValue);
    }

    public static MemoryTracker memoryTracker(KernelTransaction kernelTransaction) {
        return kernelTransaction == null ? emptyMemoryTracker() : IMPL.memoryTracker(kernelTransaction);
    }

    public static MemoryTracker emptyMemoryTracker() {
        return IMPL.emptyMemoryTracker();
    }

    @TestOnly
    public static MemoryTracker limitedMemoryTracker(long limitInBytes, long grabSizeInBytes) {
        return IMPL.limitedMemoryTracker(limitInBytes, grabSizeInBytes);
    }

    public static MemoryTrackerProxy memoryTrackerProxy(MemoryTracker memoryTracker) {
        return IMPL.memoryTrackerProxy(memoryTracker);
    }

    public static LogService logProviderForStoreAndRegister(
        Path storeLogPath,
        FileSystemAbstraction fs,
        LifeSupport lifeSupport
    ) throws IOException {
        return IMPL.logProviderForStoreAndRegister(storeLogPath, fs, lifeSupport);
    }

    public static Path metadataStore(DatabaseLayout databaseLayout) {
        return IMPL.metadataStore(databaseLayout);
    }

    public static Path homeDirectory(DatabaseLayout databaseLayout) {
        return IMPL.homeDirectory(databaseLayout);
    }

    public static BatchImporter instantiateBatchImporter(
        BatchImporterFactory factory,
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
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
        return IMPL.instantiateBatchImporter(
            factory,
            directoryStructure,
            fileSystem,
            pageCacheTracer,
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

    public static Input batchInputFrom(CompatInput compatInput) {
        return IMPL.batchInputFrom(compatInput);
    }

    public static String queryText(ExecutingQuery query) {
        return IMPL.queryText(query);
    }

    public static Log logger(
        Level level,
        ZoneId zoneId,
        DateTimeFormatter dateTimeFormatter,
        String category,
        PrintWriter writer
    ) {
        return IMPL.logger(level, zoneId, dateTimeFormatter, category, writer);
    }

    public static Log logger(
        Level level,
        ZoneId zoneId,
        DateTimeFormatter dateTimeFormatter,
        String category,
        OutputStream outputStream
    ) {
        return IMPL.logger(level, zoneId, dateTimeFormatter, category, outputStream);
    }

    public static Setting<Boolean> onlineBackupEnabled() {
        return IMPL.onlineBackupEnabled();
    }

    public static Setting<String> additionalJvm() {
        return IMPL.additionalJvm();
    }

    public static Setting<Long> memoryTransactionMaxSize() {
        return IMPL.memoryTransactionMaxSize();
    }

    public static JobRunner runnerFromScheduler(JobScheduler scheduler, Group group) {
        return IMPL.runnerFromScheduler(scheduler, group);
    }

    public static ExecutionMonitor invisibleExecutionMonitor() {
        return IMPL.invisibleExecutionMonitor();
    }

    public static UserFunctionSignature userFunctionSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        Neo4jTypes.AnyType type,
        String deprecated,
        String[] allowed,
        String description,
        String category,
        boolean caseInsensitive
    ) {
        return IMPL.userFunctionSignature(
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

    private Neo4jProxy() {
        throw new UnsupportedOperationException("No instances");
    }
}
