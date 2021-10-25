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
import org.jetbrains.annotations.TestOnly;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.ThrowingFunction;
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
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
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
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
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

public interface Neo4jProxyApi {

    GdsGraphDatabaseAPI newDb(DatabaseManagementService dbms);

    String validateExternalDatabaseName(String databaseName);

    void cacheDatabaseId(DatabaseIdRepository.Caching databaseIdRepository, NamedDatabaseId namedDatabaseId);

    AccessMode accessMode(CustomAccessMode customAccessMode);

    AccessMode newRestrictedAccessMode(AccessMode original, AccessMode.Static restricting);

    String username(AuthSubject subject);

    SecurityContext securityContext(
        String username,
        AuthSubject authSubject,
        AccessMode mode,
        String databaseName
    );

    long getHighestPossibleIdInUse(
        RecordStore<? extends AbstractBaseRecord> recordStore,
        KernelTransaction kernelTransaction
    );

    PageCursor pageFileIO(
        PagedFile pagedFile,
        long pageId,
        int pageFileFlags
    ) throws IOException;

    PagedFile pageCacheMap(
        PageCache pageCache,
        File file,
        int pageSize,
        String databaseName,
        OpenOption... openOptions
    ) throws IOException;

    Path pagedFile(PagedFile pagedFile);

    List<StoreScan<NodeLabelIndexCursor>> entityCursorScan(
        KernelTransaction transaction,
        int[] labelIds,
        int batchSize
    );

    PropertyCursor allocatePropertyCursor(KernelTransaction kernelTransaction);

    PropertyReference propertyReference(NodeCursor nodeCursor);

    PropertyReference propertyReference(RelationshipScanCursor relationshipScanCursor);

    PropertyReference noPropertyReference();

    void nodeProperties(
        KernelTransaction kernelTransaction,
        long nodeReference,
        PropertyReference reference,
        PropertyCursor cursor
    );

    void relationshipProperties(
        KernelTransaction kernelTransaction,
        long relationshipReference,
        PropertyReference reference,
        PropertyCursor cursor
    );

    NodeCursor allocateNodeCursor(KernelTransaction kernelTransaction);

    RelationshipScanCursor allocateRelationshipScanCursor(KernelTransaction kernelTransaction);

    NodeLabelIndexCursor allocateNodeLabelIndexCursor(KernelTransaction kernelTransaction);

    NodeValueIndexCursor allocateNodeValueIndexCursor(KernelTransaction kernelTransaction);

    long relationshipsReference(NodeCursor nodeCursor);

    boolean hasNodeLabelIndex(KernelTransaction kernelTransaction);

    void nodeLabelScan(KernelTransaction kernelTransaction, int label, NodeLabelIndexCursor cursor);

    StoreScan<NodeLabelIndexCursor> nodeLabelIndexScan(
        KernelTransaction transaction,
        int labelId,
        int batchSize
    );

    <C extends Cursor> StoreScan<C> scanToStoreScan(Scan<C> scan, int batchSize);

    void nodeIndexScan(
        Read dataRead,
        IndexReadSession index,
        NodeValueIndexCursor cursor,
        IndexOrder indexOrder,
        boolean needsValues
    ) throws Exception;

    CompatIndexQuery rangeIndexQuery(
        int propertyKeyId,
        double from,
        boolean fromInclusive,
        double to,
        boolean toInclusive
    );

    CompatIndexQuery rangeAllIndexQuery(int propertyKeyId);

    void nodeIndexSeek(
        Read dataRead,
        IndexReadSession index,
        NodeValueIndexCursor cursor,
        IndexOrder indexOrder,
        boolean needsValues,
        CompatIndexQuery query
    ) throws Exception;

    CompositeNodeCursor compositeNodeCursor(List<NodeLabelIndexCursor> cursors, int[] labelIds);

    OffHeapLongArray newOffHeapLongArray(long length, long defaultValue, long base);

    LongArray newChunkedLongArray(NumberArrayFactory numberArrayFactory, int size, long defaultValue);

    MemoryTrackerProxy memoryTrackerProxy(KernelTransaction kernelTransaction);

    @TestOnly
    MemoryTrackerProxy emptyMemoryTracker();

    @TestOnly
    MemoryTrackerProxy limitedMemoryTracker(long limitInBytes, long grabSizeInBytes);

    LogService logProviderForStoreAndRegister(
        Path storeLogPath,
        FileSystemAbstraction fs,
        LifeSupport lifeSupport
    ) throws IOException;

    Path metadataStore(DatabaseLayout databaseLayout);

    Path homeDirectory(DatabaseLayout databaseLayout);

    BatchImporter instantiateBatchImporter(
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
    );

    Input batchInputFrom(CompatInput compatInput);

    String queryText(ExecutingQuery query);

    Log logger(
        Level level,
        ZoneId zoneId,
        DateTimeFormatter dateTimeFormatter,
        String category,
        PrintWriter writer
    );

    Log logger(
        Level level,
        ZoneId zoneId,
        DateTimeFormatter dateTimeFormatter,
        String category,
        OutputStream outputStream
    );

    Setting<Boolean> onlineBackupEnabled();

    Setting<String> additionalJvm();

    Setting<Long> memoryTransactionMaxSize();

    JobRunner runnerFromScheduler(JobScheduler scheduler, Group group);

    ExecutionMonitor invisibleExecutionMonitor();

    UserFunctionSignature userFunctionSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        Neo4jTypes.AnyType type,
        String deprecated,
        String[] allowed,
        String description,
        String category,
        boolean caseInsensitive
    );

    ProcedureSignature procedureSignature(
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
    );

    <T> ThrowingFunction<Context, T, ProcedureException> lookupComponentProvider(
        GlobalProcedures registry,
        Class<T> cls,
        boolean safe
    );

    long getHighestPossibleNodeCount(Read read, @Nullable IdGeneratorFactory idGeneratorFactory);

    long getHighestPossibleRelationshipCount(Read read, @Nullable IdGeneratorFactory idGeneratorFactory);
}
