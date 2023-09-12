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
package org.neo4j.gds.compat._5x;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.compat.BoltTransactionRunner;
import org.neo4j.gds.compat.CompatCallableProcedure;
import org.neo4j.gds.compat.CompatExecutionMonitor;
import org.neo4j.gds.compat.CompatIndexQuery;
import org.neo4j.gds.compat.CompatInput;
import org.neo4j.gds.compat.CompatUserAggregationFunction;
import org.neo4j.gds.compat.CompositeNodeCursor;
import org.neo4j.gds.compat.CustomAccessMode;
import org.neo4j.gds.compat.GdsDatabaseLayout;
import org.neo4j.gds.compat.GdsDatabaseManagementServiceBuilder;
import org.neo4j.gds.compat.GdsGraphDatabaseAPI;
import org.neo4j.gds.compat.GlobalProcedureRegistry;
import org.neo4j.gds.compat.InputEntityIdVisitor;
import org.neo4j.gds.compat.Neo4jProxyApi;
import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.gds.compat.StoreScan;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.Scan;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public abstract class CommonNeo4jProxyImpl implements Neo4jProxyApi {
    @Override
    public GdsGraphDatabaseAPI newDb(DatabaseManagementService dbms) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public String validateExternalDatabaseName(String databaseName) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.kernel.api.security.AccessMode accessMode(CustomAccessMode customAccessMode) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public String username(org.neo4j.internal.kernel.api.security.AuthSubject subject) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.kernel.api.security.SecurityContext securityContext(
        String username,
        org.neo4j.internal.kernel.api.security.AuthSubject authSubject,
        org.neo4j.internal.kernel.api.security.AccessMode mode,
        String databaseName
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public long getHighId(org.neo4j.kernel.impl.store.RecordStore<? extends org.neo4j.kernel.impl.store.record.AbstractBaseRecord> recordStore) {
        return 0;
    }

    @Override
    public List<StoreScan<org.neo4j.internal.kernel.api.NodeLabelIndexCursor>> entityCursorScan(
        org.neo4j.kernel.api.KernelTransaction transaction,
        int[] labelIds,
        int batchSize,
        boolean allowPartitionedScan
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.kernel.api.PropertyCursor allocatePropertyCursor(org.neo4j.kernel.api.KernelTransaction kernelTransaction) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public PropertyReference propertyReference(org.neo4j.internal.kernel.api.NodeCursor nodeCursor) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public PropertyReference propertyReference(org.neo4j.internal.kernel.api.RelationshipScanCursor relationshipScanCursor) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public PropertyReference noPropertyReference() {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public void nodeProperties(
        org.neo4j.kernel.api.KernelTransaction kernelTransaction,
        long nodeReference,
        PropertyReference reference,
        org.neo4j.internal.kernel.api.PropertyCursor cursor
    ) {

    }

    @Override
    public void relationshipProperties(
        org.neo4j.kernel.api.KernelTransaction kernelTransaction,
        long relationshipReference,
        PropertyReference reference,
        org.neo4j.internal.kernel.api.PropertyCursor cursor
    ) {

    }

    @Override
    public org.neo4j.internal.kernel.api.NodeCursor allocateNodeCursor(org.neo4j.kernel.api.KernelTransaction kernelTransaction) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.kernel.api.RelationshipScanCursor allocateRelationshipScanCursor(org.neo4j.kernel.api.KernelTransaction kernelTransaction) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.kernel.api.NodeLabelIndexCursor allocateNodeLabelIndexCursor(org.neo4j.kernel.api.KernelTransaction kernelTransaction) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.kernel.api.NodeValueIndexCursor allocateNodeValueIndexCursor(org.neo4j.kernel.api.KernelTransaction kernelTransaction) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public boolean hasNodeLabelIndex(org.neo4j.kernel.api.KernelTransaction kernelTransaction) {
        return false;
    }

    @Override
    public StoreScan<org.neo4j.internal.kernel.api.NodeLabelIndexCursor> nodeLabelIndexScan(
        org.neo4j.kernel.api.KernelTransaction transaction,
        int labelId,
        int batchSize,
        boolean allowPartitionedScan
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public <C extends Cursor> StoreScan<C> scanToStoreScan(Scan<C> scan, int batchSize) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public CompatIndexQuery rangeIndexQuery(
        int propertyKeyId,
        double from,
        boolean fromInclusive,
        double to,
        boolean toInclusive
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public CompatIndexQuery rangeAllIndexQuery(int propertyKeyId) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public void nodeIndexSeek(
        org.neo4j.internal.kernel.api.Read dataRead,
        org.neo4j.internal.kernel.api.IndexReadSession index,
        org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor,
        org.neo4j.internal.schema.IndexOrder indexOrder,
        boolean needsValues,
        CompatIndexQuery query
    ) throws KernelException {

    }

    @Override
    public CompositeNodeCursor compositeNodeCursor(
        List<org.neo4j.internal.kernel.api.NodeLabelIndexCursor> cursors,
        int[] labelIds
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.batchimport.Configuration batchImporterConfig(
        int batchSize,
        int writeConcurrency,
        Optional<Long> pageCacheMemory,
        boolean highIO,
        org.neo4j.internal.batchimport.IndexConfig indexConfig
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public int writeConcurrency(org.neo4j.internal.batchimport.Configuration batchImportConfiguration) {
        return 0;
    }

    @Override
    public org.neo4j.internal.batchimport.BatchImporter instantiateBatchImporter(
        org.neo4j.internal.batchimport.BatchImporterFactory factory,
        GdsDatabaseLayout directoryStructure,
        org.neo4j.io.fs.FileSystemAbstraction fileSystem,
        org.neo4j.io.pagecache.tracing.PageCacheTracer pageCacheTracer,
        org.neo4j.internal.batchimport.Configuration configuration,
        org.neo4j.logging.internal.LogService logService,
        org.neo4j.internal.batchimport.staging.ExecutionMonitor executionMonitor,
        org.neo4j.internal.batchimport.AdditionalInitialIds additionalInitialIds,
        org.neo4j.configuration.Config dbConfig,
        org.neo4j.kernel.impl.store.format.RecordFormats recordFormats,
        org.neo4j.scheduler.JobScheduler jobScheduler,
        org.neo4j.internal.batchimport.input.Collector badCollector
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.batchimport.input.Input batchInputFrom(CompatInput compatInput) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public InputEntityIdVisitor.Long inputEntityLongIdVisitor(
        org.neo4j.internal.batchimport.input.IdType idType,
        org.neo4j.internal.batchimport.input.ReadableGroups groups
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public InputEntityIdVisitor.String inputEntityStringIdVisitor(org.neo4j.internal.batchimport.input.ReadableGroups groups) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.graphdb.config.Setting<String> additionalJvm() {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.graphdb.config.Setting<?> pageCacheMemory() {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public Object pageCacheMemoryValue(String value) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.kernel.api.procs.ProcedureSignature procedureSignature(
        org.neo4j.internal.kernel.api.procs.QualifiedName name,
        List<org.neo4j.internal.kernel.api.procs.FieldSignature> inputSignature,
        List<org.neo4j.internal.kernel.api.procs.FieldSignature> outputSignature,
        org.neo4j.procedure.Mode mode,
        boolean admin,
        String deprecated,
        String description,
        String warning,
        boolean eager,
        boolean caseInsensitive,
        boolean systemProcedure,
        boolean internal,
        boolean allowExpiredCredentials,
        boolean threadSafe
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public long getHighestPossibleNodeCount(
        org.neo4j.internal.kernel.api.Read read,
        org.neo4j.internal.id.IdGeneratorFactory idGeneratorFactory
    ) {
        return 0;
    }

    @Override
    public long getHighestPossibleRelationshipCount(
        org.neo4j.internal.kernel.api.Read read,
        org.neo4j.internal.id.IdGeneratorFactory idGeneratorFactory
    ) {
        return 0;
    }

    @Override
    public String versionLongToString(long storeVersion) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public TestLog testLog() {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.logging.Log getUserLog(org.neo4j.logging.internal.LogService logService, Class<?> loggingClass) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.logging.Log getInternalLog(
        org.neo4j.logging.internal.LogService logService,
        Class<?> loggingClass
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.values.virtual.NodeValue nodeValue(
        long id,
        org.neo4j.values.storable.TextArray labels,
        org.neo4j.values.virtual.MapValue properties
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.graphdb.Relationship virtualRelationship(
        long id,
        org.neo4j.graphdb.Node startNode,
        org.neo4j.graphdb.Node endNode,
        org.neo4j.graphdb.RelationshipType type
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public GdsDatabaseManagementServiceBuilder databaseManagementServiceBuilder(Path storeDir) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.kernel.impl.store.format.RecordFormats selectRecordFormatForStore(
        org.neo4j.io.layout.DatabaseLayout databaseLayout,
        org.neo4j.io.fs.FileSystemAbstraction fs,
        org.neo4j.io.pagecache.PageCache pageCache,
        org.neo4j.logging.internal.LogService logService,
        org.neo4j.io.pagecache.tracing.PageCacheTracer pageCacheTracer
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public boolean isNotNumericIndex(org.neo4j.internal.schema.IndexCapability indexCapability) {
        return false;
    }

    @Override
    public void setAllowUpgrades(org.neo4j.configuration.Config.Builder configBuilder, boolean value) {

    }

    @Override
    public String defaultRecordFormatSetting() {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public void configureRecordFormat(org.neo4j.configuration.Config.Builder configBuilder, String recordFormat) {

    }

    @Override
    public GdsDatabaseLayout databaseLayout(org.neo4j.configuration.Config config, String databaseName) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.io.layout.Neo4jLayout neo4jLayout(org.neo4j.configuration.Config config) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public BoltTransactionRunner<?, ?> boltTransactionRunner() {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.helpers.HostnamePort getLocalBoltAddress(org.neo4j.configuration.connectors.ConnectorPortRegister connectorPortRegister) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.ssl.config.SslPolicyLoader createSllPolicyLoader(
        org.neo4j.io.fs.FileSystemAbstraction fileSystem,
        org.neo4j.configuration.Config config,
        org.neo4j.logging.internal.LogService logService
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.kernel.impl.store.format.RecordFormats recordFormatSelector(
        String databaseName,
        org.neo4j.configuration.Config databaseConfig,
        org.neo4j.io.fs.FileSystemAbstraction fs,
        org.neo4j.logging.internal.LogService logService,
        org.neo4j.graphdb.GraphDatabaseService databaseService
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.batchimport.staging.ExecutionMonitor executionMonitor(CompatExecutionMonitor compatExecutionMonitor) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.internal.kernel.api.procs.UserFunctionSignature userFunctionSignature(
        org.neo4j.internal.kernel.api.procs.QualifiedName name,
        List<org.neo4j.internal.kernel.api.procs.FieldSignature> inputSignature,
        org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType type,
        String description,
        boolean internal,
        boolean threadSafe,
        Optional<String> deprecatedBy
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.kernel.api.procedure.CallableProcedure callableProcedure(CompatCallableProcedure procedure) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public org.neo4j.kernel.api.procedure.CallableUserAggregationFunction callableUserAggregationFunction(
        CompatUserAggregationFunction function
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public long transactionId(org.neo4j.kernel.api.KernelTransactionHandle kernelTransactionHandle) {
        return 0;
    }

    @Override
    public void reserveNeo4jIds(
        org.neo4j.internal.id.IdGeneratorFactory generatorFactory,
        int size,
        org.neo4j.io.pagecache.context.CursorContext cursorContext
    ) {

    }

    @Override
    public org.neo4j.kernel.impl.query.TransactionalContext newQueryContext(
        org.neo4j.kernel.impl.query.TransactionalContextFactory contextFactory,
        org.neo4j.kernel.impl.coreapi.InternalTransaction tx,
        String queryText,
        org.neo4j.values.virtual.MapValue queryParameters
    ) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public boolean isCompositeDatabase(org.neo4j.graphdb.GraphDatabaseService databaseService) {
        return false;
    }

    @Override
    public <T> T lookupComponentProvider(
        org.neo4j.kernel.api.procedure.Context ctx,
        Class<T> component,
        boolean safe
    ) throws org.neo4j.internal.kernel.api.exceptions.ProcedureException {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    @Override
    public GlobalProcedureRegistry globalProcedureRegistry(org.neo4j.kernel.api.procedure.GlobalProcedures globalProcedures) {
        throw new IllegalStateException("Compat layer for 5.x must be run on Java 17");
    }

    public abstract CursorContextFactory cursorContextFactory(Optional<PageCacheTracer> pageCacheTracer);
}
