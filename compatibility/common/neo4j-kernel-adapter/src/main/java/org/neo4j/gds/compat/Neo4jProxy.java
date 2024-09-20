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

import org.intellij.lang.annotations.PrintFormat;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.compat.batchimport.BatchImporter;
import org.neo4j.gds.compat.batchimport.ImportConfig;
import org.neo4j.gds.compat.batchimport.Monitor;
import org.neo4j.gds.compat.batchimport.input.Collector;
import org.neo4j.gds.compat.batchimport.input.Estimates;
import org.neo4j.gds.compat.batchimport.input.ReadableGroups;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.SequenceValue;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;

public final class Neo4jProxy {

    private static final Neo4jProxyApi IMPL = ProxyUtil.findProxy(
        Neo4jProxyFactory.class,
        ProxyUtil.MayLogToStdout.YES
    );

    public static <T> T lookupComponentProvider(Context ctx, Class<T> component, boolean safe)
        throws ProcedureException {
        var globalProcedures = GraphDatabaseApiProxy.resolveDependency(
            ctx.dependencyResolver(),
            GlobalProcedures.class
        );
        return globalProcedures.getCurrentView().lookupComponentProvider(component, safe).apply(ctx);
    }

    public static boolean hasNodeLabelIndex(KernelTransaction kernelTransaction) {
        return NodeLabelIndexLookupImpl.hasNodeLabelIndex(kernelTransaction);
    }

    public static StoreScan<NodeCursor> nodesScan(KernelTransaction ktx, long nodeCount, int batchSize) {
        int numberOfPartitions = PartitionedStoreScan.getNumberOfPartitions(nodeCount, batchSize);
        return new PartitionedStoreScan<>(ktx.dataRead().allNodesScan(numberOfPartitions, ktx.cursorContext()));
    }

    public static StoreScan<RelationshipScanCursor> relationshipsScan(
        KernelTransaction ktx,
        long relationshipCount,
        int batchSize
    ) {
        int numberOfPartitions = PartitionedStoreScan.getNumberOfPartitions(relationshipCount, batchSize);
        return new PartitionedStoreScan<>(ktx.dataRead().allRelationshipsScan(numberOfPartitions, ktx.cursorContext()));
    }

    public static CompositeNodeCursor compositeNodeCursor(List<NodeLabelIndexCursor> cursors, int[] labelIds) {
        return new CompositeNodeCursorImpl(cursors, labelIds);
    }

    public static BatchImporter instantiateBatchImporter(
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        ImportConfig configuration,
        LogService logService,
        Monitor executionMonitor,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        return IMPL.instantiateBatchImporter(
            directoryStructure,
            fileSystem,
            configuration,
            executionMonitor,
            logService,
            dbConfig,
            jobScheduler,
            badCollector
        );
    }

    public static long getHighestPossibleNodeCount(IdGeneratorFactory idGeneratorFactory) {
        return InternalReadOps.findValidIdGeneratorsStream(
                idGeneratorFactory,
                RecordIdType.NODE,
                BlockFormat.INSTANCE.nodeType,
                BlockFormat.INSTANCE.dynamicNodeType
            )
            .mapToLong(IdGenerator::getHighId)
            .max()
            .orElseThrow(InternalReadOps::unsupportedStoreFormatException);
    }

    public static ReadableGroups newGroups() {
        return IMPL.newGroups();
    }

    public static ReadableGroups newInitializedGroups() {
        return IMPL.newInitializedGroups();
    }

    public static Collector emptyCollector() {
        return IMPL.emptyCollector();
    }

    public static Collector badCollector(OutputStream log, int batchSize) {
        return IMPL.badCollector(log, batchSize);
    }

    public static Estimates knownEstimates(
        long numberOfNodes,
        long numberOfRelationships,
        long numberOfNodeProperties,
        long numberOfRelationshipProperties,
        long sizeOfNodeProperties,
        long sizeOfRelationshipProperties,
        long numberOfNodeLabels
    ) {
        return IMPL.knownEstimates(
            numberOfNodes,
            numberOfRelationships,
            numberOfNodeProperties,
            numberOfRelationshipProperties,
            sizeOfNodeProperties,
            sizeOfRelationshipProperties,
            numberOfNodeLabels
        );
    }

    private static final class BlockFormat {
        private static final BlockFormat INSTANCE = new BlockFormat();

        private org.neo4j.internal.id.IdType nodeType = null;
        private org.neo4j.internal.id.IdType dynamicNodeType = null;

        BlockFormat() {
            try {
                var blockIdType = Class.forName("com.neo4j.internal.blockformat.BlockIdType");
                var blockTypes = Objects.requireNonNull(blockIdType.getEnumConstants());
                for (Object blockType : blockTypes) {
                    var type = (Enum<?>) blockType;
                    switch (type.name()) {
                        case "NODE" -> this.nodeType = (org.neo4j.internal.id.IdType) type;
                        case "DYNAMIC_NODE" -> this.dynamicNodeType = (org.neo4j.internal.id.IdType) type;
                    }
                }
            } catch (ClassNotFoundException | NullPointerException | ClassCastException ignored) {
            }
        }
    }

    public static String versionLongToString(long storeVersion) {
        // copied from org.neo4j.kernel.impl.store.LegacyMetadataHandler.versionLongToString which is private
        if (storeVersion == -1) {
            return "Unknown";
        }
        var bits = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(storeVersion).rewind();
        int length = bits.get() & 0xFF;
        if (length == 0 || length > 7) {
            throw new IllegalArgumentException(
                format(
                    Locale.ENGLISH,
                    "The read version string length %d is not proper.",
                    length
                )
            );
        }
        char[] result = new char[length];
        for (int i = 0; i < length; i++) {
            result[i] = (char) (bits.get() & 0xFF);
        }
        return new String(result);
    }

    public static TestLog testLog() {
        return new TestLogImpl();
    }

    public static Relationship virtualRelationship(long id, Node startNode, Node endNode, RelationshipType type) {
        return new VirtualRelationshipImpl(id, startNode, endNode, type);
    }

    public static GdsDatabaseManagementServiceBuilder databaseManagementServiceBuilder(Path storeDir) {
        return new GdsDatabaseManagementServiceBuilderImpl(storeDir);
    }

    public static String defaultDatabaseFormatSetting() {
        return migratedDefaultDatabaseFormatSetting().orElseGet(GraphDatabaseSettings.db_format::defaultValue);
    }

    private static Optional<String> migratedDefaultDatabaseFormatSetting() {
        try {
            Class<?> cls = Class.forName("com.neo4j.configuration.DefaultDbFormatSettingMigrator");
            Object migrator = cls.getDeclaredConstructor().newInstance();
            var migrateMethod = cls.getDeclaredMethod("migrate", Map.class, Map.class, InternalLog.class);
            var defaultValues = new HashMap<String, String>();
            migrateMethod.invoke(migrator, Map.<String, String>of(), defaultValues, null);
            return Optional.ofNullable(defaultValues.get(GraphDatabaseSettings.db_format.name()));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @SuppressForbidden(reason = "This is the compat API")
    public static CallableProcedure callableProcedure(CompatCallableProcedure procedure) {
        return IMPL.callableProcedure(procedure);
    }

    public static int sequenceSizeAsInt(SequenceValue listValue) {
        return IMPL.sequenceSizeAsInt(listValue);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static RuntimeException queryExceptionAsRuntimeException(Throwable e) {
        return IMPL.queryExceptionAsRuntimeException(e);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static ProcedureException procedureCallFailed(@PrintFormat String message, Object... args) {
        return IMPL.procedureCallFailed(message, args);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static ProcedureException procedureCallFailed(
        Throwable reason,
        @PrintFormat String message,
        Object... args
    ) {
        return IMPL.procedureCallFailed(reason, message, args);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static DatabaseNotFoundException databaseNotFoundException(String message) {
        return IMPL.databaseNotFoundException(message);
    }

    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    public static String exceptionMessage(Throwable e) {
        return IMPL.exceptionMessage(e);
    }

    public static void reserveNeo4jIds(IdGeneratorFactory generatorFactory, int size, CursorContext cursorContext) {
        var idGenerator = InternalReadOps.findValidIdGeneratorsStream(
                generatorFactory,
                RecordIdType.NODE,
                BlockFormat.INSTANCE.nodeType,
                BlockFormat.INSTANCE.dynamicNodeType
            )
            .findFirst().orElseThrow(InternalReadOps::unsupportedStoreFormatException);

        idGenerator.nextConsecutiveIdRange(size, false, cursorContext);
    }


    /**
     * The implementations of this method should look identical and are source-compatible.
     * However, Since 5.24, Neo4j exceptions implement `HasGqlStatusInfo`, which requires
     * a new module dependency that doesn't exist in versions before 5.24.
     * In order to access any methods on exceptions, we need to do so behind the compat layer.
     */
    static void rethrowUnlessDuplicateRegistration(ProcedureException e) throws KernelException {
        IMPL.rethrowUnlessDuplicateRegistration(e);
    }

    private Neo4jProxy() {
        throw new UnsupportedOperationException("No instances");
    }
}
