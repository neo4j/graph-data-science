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
package org.neo4j.gds.compat._56;

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.compat.BoltTransactionRunner;
import org.neo4j.gds.compat.CompatExecutionContext;
import org.neo4j.gds.compat.GlobalProcedureRegistry;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.StoreScan;
import org.neo4j.gds.compat._5x.CommonNeo4jProxyImpl;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.procedure.Mode;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Neo4jProxyImpl extends CommonNeo4jProxyImpl {

    @Override
    public long getHighId(RecordStore<? extends AbstractBaseRecord> recordStore) {
        return recordStore.getHighId();
    }

    @Override
    public <T> T lookupComponentProvider(Context ctx, Class<T> component, boolean safe) throws ProcedureException {
        var globalProcedures = GraphDatabaseApiProxy.resolveDependency(
            ctx.dependencyResolver(),
            GlobalProcedures.class
        );
        return globalProcedures.lookupComponentProvider(component, safe).apply(ctx);
    }

    @Override
    public List<StoreScan<NodeLabelIndexCursor>> entityCursorScan(
        KernelTransaction transaction,
        int[] labelIds,
        int batchSize,
        boolean allowPartitionedScan
    ) {
        if (allowPartitionedScan) {
            return this.partitionedCursorScan(transaction, batchSize, labelIds);
        } else {
            var read = transaction.dataRead();
            return Arrays
                .stream(labelIds)
                .mapToObj(read::nodeLabelScan)
                .map(scan -> new ScanBasedStoreScanImpl<>(scan, batchSize))
                .collect(Collectors.toList());
        }
    }

    @Override
    public StoreScan<NodeLabelIndexCursor> nodeLabelIndexScan(
        KernelTransaction transaction,
        int labelId,
        int batchSize,
        boolean allowPartitionedScan
    ) {
        if (allowPartitionedScan) {
            return this.partitionedCursorScan(transaction, batchSize, labelId).get(0);
        } else {
            var read = transaction.dataRead();
            var scan = read.nodeLabelScan(labelId);
            return new ScanBasedStoreScanImpl<>(scan, batchSize);
        }
    }

    @Override
    public StoreScan<NodeCursor> nodesScan(KernelTransaction ktx, long nodeCount, int batchSize) {
        return new ScanBasedStoreScanImpl<>(ktx.dataRead().allNodesScan(), batchSize);
    }

    @Override
    public StoreScan<RelationshipScanCursor> relationshipsScan(
        KernelTransaction ktx,
        long relationshipCount,
        int batchSize
    ) {
        return new ScanBasedStoreScanImpl<>(ktx.dataRead().allRelationshipsScan(), batchSize);
    }

    @Override
    public BoltTransactionRunner<?, ?> boltTransactionRunner() {
        return new BoltTransactionRunnerImpl();
    }

    @Override
    public ProcedureSignature procedureSignature(
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
        boolean allowExpiredCredentials,
        boolean threadSafe
    ) {
        return new ProcedureSignature(
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

    @Override
    public GlobalProcedureRegistry globalProcedureRegistry(GlobalProcedures globalProcedures) {
        return new GlobalProcedureRegistry() {
            @Override
            public Set<ProcedureSignature> getAllProcedures() {
                return globalProcedures.getAllProcedures();
            }

            @Override
            public Stream<UserFunctionSignature> getAllNonAggregatingFunctions() {
                return globalProcedures.getAllNonAggregatingFunctions();
            }

            @Override
            public Stream<UserFunctionSignature> getAllAggregatingFunctions() {
                return globalProcedures.getAllAggregatingFunctions();
            }
        };
    }

    private static final DependencyResolver EMPTY_DEPENDENCY_RESOLVER = new DependencyResolver.Adapter() {
        @Override
        public <T> T resolveDependency(Class<T> type, SelectionStrategy selector) {
            return null;
        }

        @Override
        public boolean containsDependency(Class<?> type) {
            return false;
        }
    };

    @Override
    public DependencyResolver emptyDependencyResolver() {
        return EMPTY_DEPENDENCY_RESOLVER;
    }

    @Override
    public CursorContextFactory cursorContextFactory(Optional<PageCacheTracer> pageCacheTracer) {
        return pageCacheTracer.map(cacheTracer -> new CursorContextFactory(
            cacheTracer,
            EmptyVersionContextSupplier.EMPTY
        )).orElse(CursorContextFactory.NULL_CONTEXT_FACTORY);
    }

    @Override
    public CompatExecutionContext executionContext(KernelTransaction ktx) {
        return new CompatExecutionContext() {
            @Override
            public CursorContext cursorContext() {
                return ktx.cursorContext();
            }

            @Override
            public AccessMode accessMode() {
                return ktx.securityContext().mode();
            }

            @Override
            public <C extends Cursor> boolean reservePartition(PartitionedScan<C> scan, C cursor) {
                return scan.reservePartition(cursor, cursorContext(), accessMode());
            }

            @Override
            public void close() {
            }
        };
    }
}
