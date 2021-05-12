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
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphalgo.annotation.SuppressForbidden;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.cache.LongArray;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.cache.OffHeapLongArray;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
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
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.Mode;
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
import java.util.Optional;
import java.util.ServiceLoader;

@SuppressForbidden(reason = "This is the best we can do at the moment")
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
        var log = LogBuilders.outputStreamLog(
            System.out,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
        log.info("Loaded compatibility layer: %s", IMPL.getClass());
        log.info("Loaded version: %s", neo4jVersion);
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

    // Maybe we should move this to a test-only proxy?
    @TestOnly
    public static SecurityContext securityContext(
        String username,
        AuthSubject authSubject,
        AccessMode mode
    ) {
        return IMPL.securityContext(username, authSubject, mode);
    }

    public static long getHighestPossibleIdInUse(
        RecordStore<? extends AbstractBaseRecord> recordStore,
        KernelTransaction kernelTransaction
    ) {
        return IMPL.getHighestPossibleIdInUse(recordStore, kernelTransaction);
    }

    public static PageCursor pageFileIO(
        PagedFile pagedFile,
        long pageId,
        int pageFileFlags
    ) throws IOException {
        return IMPL.pageFileIO(pagedFile, pageId, pageFileFlags);
    }

    public static PagedFile pageCacheMap(
        PageCache pageCache,
        File file,
        int pageSize,
        String databaseName,
        OpenOption... openOptions
    )
    throws IOException {
        return IMPL.pageCacheMap(pageCache, file, pageSize, databaseName, openOptions);
    }

    public static Path pagedFile(PagedFile pagedFile) {
        return IMPL.pagedFile(pagedFile);
    }

    public static PropertyCursor allocatePropertyCursor(KernelTransaction kernelTransaction) {
        return IMPL.allocatePropertyCursor(kernelTransaction);
    }

    public static NodeCursor allocateNodeCursor(KernelTransaction kernelTransaction) {
        return IMPL.allocateNodeCursor(kernelTransaction);
    }

    public static RelationshipScanCursor allocateRelationshipScanCursor(KernelTransaction kernelTransaction) {
        return IMPL.allocateRelationshipScanCursor(kernelTransaction);
    }

    public static NodeLabelIndexCursor allocateNodeLabelIndexCursor(KernelTransaction kernelTransaction) {
        return IMPL.allocateNodeLabelIndexCursor(kernelTransaction);
    }

    public static NodeValueIndexCursor allocateNodeValueIndexCursor(KernelTransaction kernelTransaction) {
        return IMPL.allocateNodeValueIndexCursor(kernelTransaction);
    }

    public static long relationshipsReference(NodeCursor nodeCursor) {
        return IMPL.relationshipsReference(nodeCursor);
    }

    public static void nodeLabelScan(KernelTransaction kernelTransaction, int label, NodeLabelIndexCursor cursor) {
        IMPL.nodeLabelScan(kernelTransaction, label, cursor);
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

    public static MemoryTrackerProxy memoryTrackerProxy(KernelTransaction kernelTransaction) {
        return IMPL.memoryTrackerProxy(kernelTransaction);
    }

    @TestOnly
    public static MemoryTrackerProxy emptyMemoryTrackerProxy() {
        return IMPL.emptyMemoryTracker();
    }

    @TestOnly
    public static MemoryTrackerProxy limitedMemoryTrackerProxy(long limitInBytes, long grabSizeInBytes) {
        return IMPL.limitedMemoryTracker(limitInBytes, grabSizeInBytes);
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
        return IMPL.instantiateBatchImporter(
            factory,
            directoryStructure,
            fileSystem,
            pageCacheTracer,
            writeConcurrency,
            pageCacheMemory,
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

    public static ProcedureSignature procedureSignature(
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
        return IMPL.procedureSignature(
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

    public static <T> ThrowingFunction<Context, T, ProcedureException> lookupComponentProvider(
        GlobalProcedures registry,
        Class<T> cls,
        boolean safe
    ) {
        return IMPL.lookupComponentProvider(registry, cls, safe);
    }

    private Neo4jProxy() {
        throw new UnsupportedOperationException("No instances");
    }
}
