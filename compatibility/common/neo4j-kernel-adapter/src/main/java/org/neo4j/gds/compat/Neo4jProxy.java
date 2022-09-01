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
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IndexConfig;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.helpers.HostnamePort;
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
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.Mode;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public final class Neo4jProxy {

    private static final Neo4jProxyApi IMPL = ProxyUtil.findProxy(Neo4jProxyFactory.class);

    public static GdsGraphDatabaseAPI newDb(DatabaseManagementService dbms) {
        return IMPL.newDb(dbms);
    }

    public static String validateExternalDatabaseName(String databaseName) {
        return IMPL.validateExternalDatabaseName(databaseName);
    }

    public static AccessMode accessMode(CustomAccessMode customAccessMode) {
        return IMPL.accessMode(customAccessMode);
    }

    public static String username(AuthSubject subject) {
        return IMPL.username(subject);
    }

    // Maybe we should move this to a test-only proxy?
    @TestOnly
    public static SecurityContext securityContext(
        String username,
        AuthSubject authSubject,
        AccessMode mode,
        String databaseName
    ) {
        return IMPL.securityContext(username, authSubject, mode, databaseName);
    }

    public static long getHighestPossibleIdInUse(
        RecordStore<? extends AbstractBaseRecord> recordStore,
        KernelTransaction kernelTransaction
    ) {
        return IMPL.getHighestPossibleIdInUse(recordStore, kernelTransaction);
    }

    public static List<StoreScan<NodeLabelIndexCursor>> entityCursorScan(
        KernelTransaction transaction,
        int[] labelIds,
        int batchSize,
        boolean allowPartitionedScan
    ) {
        return IMPL.entityCursorScan(transaction, labelIds, batchSize, allowPartitionedScan);
    }

    public static PropertyCursor allocatePropertyCursor(KernelTransaction kernelTransaction) {
        return IMPL.allocatePropertyCursor(kernelTransaction);
    }

    public static PropertyReference propertyReference(NodeCursor nodeCursor) {
        return IMPL.propertyReference(nodeCursor);
    }

    public static PropertyReference propertyReference(RelationshipScanCursor relationshipScanCursor) {
        return IMPL.propertyReference(relationshipScanCursor);
    }

    public static PropertyReference noPropertyReference() {
        return IMPL.noPropertyReference();
    }

    public static void nodeProperties(
        KernelTransaction kernelTransaction,
        long nodeReference,
        PropertyReference reference,
        PropertyCursor cursor
    ) {
        IMPL.nodeProperties(kernelTransaction, nodeReference, reference, cursor);
    }

    public static void relationshipProperties(
        KernelTransaction kernelTransaction,
        long relationshipReference,
        PropertyReference reference,
        PropertyCursor cursor
    ) {
        IMPL.relationshipProperties(kernelTransaction, relationshipReference, reference, cursor);
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

    public static boolean hasNodeLabelIndex(KernelTransaction kernelTransaction) {
        return IMPL.hasNodeLabelIndex(kernelTransaction);
    }

    public static StoreScan<NodeLabelIndexCursor> nodeLabelIndexScan(
        KernelTransaction transaction,
        int labelId,
        int batchSize,
        boolean allowPartitionedScan
    ) {
        return IMPL.nodeLabelIndexScan(transaction, labelId, batchSize, allowPartitionedScan);
    }

    public static <C extends Cursor> StoreScan<C> scanToStoreScan(Scan<C> scan, int batchSize) {
        return IMPL.scanToStoreScan(scan, batchSize);
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
    ) throws KernelException {
        IMPL.nodeIndexSeek(dataRead, index, cursor, indexOrder, needsValues, query);
    }

    public static CompositeNodeCursor compositeNodeCursor(List<NodeLabelIndexCursor> cursors, int[] labelIds) {
        return IMPL.compositeNodeCursor(cursors, labelIds);
    }

    public static Configuration batchImporterConfig(
        int batchSize,
        int writeConcurrency,
        Optional<Long> pageCacheMemory,
        boolean highIO,
        IndexConfig indexConfig
    ) {
        return IMPL.batchImporterConfig(batchSize, writeConcurrency, pageCacheMemory, highIO, indexConfig);
    }

    @TestOnly
    public static int writeConcurrency(Configuration batchImporterConfiguration) {
        return IMPL.writeConcurrency(batchImporterConfiguration);
    }

    public static BatchImporter instantiateBatchImporter(
        BatchImporterFactory factory,
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        PageCacheTracer pageCacheTracer,
        Configuration configuration,
        LogService logService,
        ExecutionMonitor executionMonitor,
        AdditionalInitialIds additionalInitialIds,
        Config dbConfig,
        RecordFormats recordFormats,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        return IMPL.instantiateBatchImporter(
            factory,
            directoryStructure,
            fileSystem,
            pageCacheTracer,
            configuration,
            logService,
            executionMonitor,
            additionalInitialIds,
            dbConfig,
            recordFormats,
            jobScheduler,
            badCollector
        );
    }

    public static Input batchInputFrom(CompatInput compatInput) {
        return IMPL.batchInputFrom(compatInput);
    }

    public static Setting<String> additionalJvm() {
        return IMPL.additionalJvm();
    }

    @SuppressWarnings("unchecked")
    public static <T> Setting<T> pageCacheMemory() {
        return (Setting<T>) IMPL.pageCacheMemory();
    }

    @SuppressWarnings("unchecked")
    public static <T> T pageCacheMemoryValue(String value) {
        return (T) IMPL.pageCacheMemoryValue(value);
    }

    public static ExecutionMonitor invisibleExecutionMonitor() {
        return IMPL.invisibleExecutionMonitor();
    }

    public static ProcedureSignature procedureSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        List<FieldSignature> outputSignature,
        Mode mode,
        boolean admin,
        String deprecated,
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
            description,
            warning,
            eager,
            caseInsensitive,
            systemProcedure,
            internal,
            allowExpiredCredentials
        );
    }

    public static long getHighestPossibleNodeCount(Read read, @Nullable IdGeneratorFactory idGeneratorFactory) {
        return IMPL.getHighestPossibleNodeCount(read, idGeneratorFactory);
    }

    public static long getHighestPossibleRelationshipCount(Read read, @Nullable IdGeneratorFactory idGeneratorFactory) {
        return IMPL.getHighestPossibleRelationshipCount(read, idGeneratorFactory);
    }

    public static String versionLongToString(long storeVersion) {
        return IMPL.versionLongToString(storeVersion);
    }

    public static TestLog testLog() {
        return IMPL.testLog();
    }

    public static Log getUserLog(LogService logService, Class<?> loggingClass) {
        return IMPL.getUserLog(logService, loggingClass);
    }

    public static Log getInternalLog(LogService logService, Class<?> loggingClass) {
        return IMPL.getInternalLog(logService, loggingClass);
    }

    public static Relationship virtualRelationship(long id, Node startNode, Node endNode, RelationshipType type) {
        return IMPL.virtualRelationship(id, startNode, endNode, type);
    }

    public static GdsDatabaseManagementServiceBuilder databaseManagementServiceBuilder(Path storeDir) {
        return IMPL.databaseManagementServiceBuilder(storeDir);
    }

    public static RecordFormats selectRecordFormatForStore(
        DatabaseLayout databaseLayout,
        FileSystemAbstraction fs,
        PageCache pageCache,
        LogService logService,
        PageCacheTracer pageCacheTracer
    ) {
        return IMPL.selectRecordFormatForStore(databaseLayout, fs, pageCache, logService, pageCacheTracer);
    }

    public static boolean isNotNumericIndex(IndexCapability indexCapability) {
        return IMPL.isNotNumericIndex(indexCapability);
    }

    public static void setAllowUpgrades(Config.Builder configBuilder, boolean value) {
        IMPL.setAllowUpgrades(configBuilder, value);
    }

    public static String defaultRecordFormatSetting() {
        return IMPL.defaultRecordFormatSetting();
    }

    public static void configureRecordFormat(Config.Builder configBuilder, String recordFormat) {
        IMPL.configureRecordFormat(configBuilder, recordFormat);
    }

    public static DatabaseLayout databaseLayout(Config config, String databaseName) {
        return IMPL.databaseLayout(config, databaseName);
    }

    public static BoltTransactionRunner<?, ?> boltTransactionRunner() {
        return IMPL.boltTransactionRunner();
    }

    public static HostnamePort getLocalBoltAddress(ConnectorPortRegister connectorPortRegister) {
        return IMPL.getLocalBoltAddress(connectorPortRegister);
    }

    public static SslPolicyLoader createSllPolicyLoader(
        FileSystemAbstraction fileSystem,
        Config config,
        LogService logService
    ) {
        return IMPL.createSllPolicyLoader(fileSystem, config, logService);
    }

    public static RecordFormats recordFormatSelector(
        String databaseName,
        Config databaseConfig,
        FileSystemAbstraction fs,
        LogService logService,
        GraphDatabaseService databaseService
    ) {
        return IMPL.recordFormatSelector(databaseName, databaseConfig, fs, logService, databaseService);
    }

    public static NamedDatabaseId randomDatabaseId() {
        return IMPL.randomDatabaseId();
    }

    public static ExecutionMonitor executionMonitor(CompatExecutionMonitor compatExecutionMonitor) {
        return IMPL.executionMonitor(compatExecutionMonitor);
    }

    public static UserFunctionSignature userFunctionSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        Neo4jTypes.AnyType type,
        String description,
        boolean internal
    ) {
        return IMPL.userFunctionSignature(name, inputSignature, type, description, internal);
    }

    public static long transactionId(KernelTransactionHandle kernelTransactionHandle) {
        return IMPL.transactionId(kernelTransactionHandle);
    }

    public static void reserveNeo4jIds(IdGeneratorFactory generatorFactory, int size, CursorContext cursorContext) {
        IMPL.reserveNeo4jIds(generatorFactory, size, cursorContext);
    }

    private Neo4jProxy() {
        throw new UnsupportedOperationException("No instances");
    }
}
