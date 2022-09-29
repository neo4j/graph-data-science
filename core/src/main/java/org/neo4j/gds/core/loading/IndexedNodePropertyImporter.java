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
package org.neo4j.gds.core.loading;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.ArrayUtil;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.compat.CompatIndexQuery;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.StatementAction;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.NumberValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutorService;

public final class IndexedNodePropertyImporter extends StatementAction {
    private final int concurrency;
    private final NodeLabel nodeLabel;
    private final PropertyMapping mapping;
    private final IndexDescriptor index;
    private final Optional<CompatIndexQuery> indexQuery;
    private final IdMap idMap;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private final @Nullable ExecutorService executorService;
    private final int propertyId;
    private final NodePropertiesFromStoreBuilder propertiesBuilder;
    private long imported;
    private long logged;

    IndexedNodePropertyImporter(
        int concurrency,
        TransactionContext tx,
        NodeLabel nodeLabel,
        PropertyMapping mapping,
        IndexDescriptor index,
        IdMap idMap,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        @Nullable ExecutorService executorService,
        NodePropertiesFromStoreBuilder propertiesBuilder
    ) {
        this(
            concurrency,
            tx,
            nodeLabel,
            mapping,
            index,
            Optional.empty(),
            idMap,
            progressTracker,
            terminationFlag,
            executorService,
            index.schema().getPropertyId(),
            propertiesBuilder
        );
    }

    private IndexedNodePropertyImporter(IndexedNodePropertyImporter from, CompatIndexQuery indexQuery) {
        this(
            from.concurrency,
            from.tx,
            from.nodeLabel,
            from.mapping,
            from.index,
            Optional.of(indexQuery),
            from.idMap,
            from.progressTracker,
            from.terminationFlag,
            from.executorService,
            from.propertyId,
            from.propertiesBuilder
        );
    }

    private IndexedNodePropertyImporter(
        int concurrency,
        TransactionContext tx,
        NodeLabel nodeLabel,
        PropertyMapping mapping,
        IndexDescriptor index,
        Optional<CompatIndexQuery> indexQuery,
        IdMap idMap,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        @Nullable ExecutorService executorService,
        int propertyId,
        NodePropertiesFromStoreBuilder propertiesBuilder
    ) {
        super(tx);
        this.concurrency = concurrency;
        this.nodeLabel = nodeLabel;
        this.mapping = mapping;
        this.index = index;
        this.indexQuery = indexQuery;
        this.idMap = idMap;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.executorService = executorService;
        this.propertyId = propertyId;
        this.propertiesBuilder = propertiesBuilder;
    }

    @Override
    public String threadName() {
        return "index-scan-" + index.getName();
    }

    @Override
    public void accept(KernelTransaction ktx) throws Exception {
        var read = ktx.dataRead();
        try (var indexCursor = Neo4jProxy.allocateNodeValueIndexCursor(ktx)) {
            var propertyIds = index.schema().getPropertyIds();
            var propertyOffset = ArrayUtil.linearSearchIndex(propertyIds, propertyIds.length, propertyId);
            // if the looked for propertyId is not there we return
            // this is not expected to happen, but it is safe
            if (propertyOffset < 0) {
                return;
            }

            var indexReadSession = read.indexReadSession(index);
            if (indexQuery.isPresent()) {
                // if indexQuery is not null, we are a parallel batch
                Neo4jProxy.nodeIndexSeek(read, indexReadSession, indexCursor, IndexOrder.NONE, true, indexQuery.get());
            } else {
                // we don't need to check the feature flag, as we set the concurrency to 1 in ScanningNodesImporter
                if (concurrency > 1 && ParallelUtil.canRunInParallel(executorService)) {
                    // try to import in parallel, see if we can find a range
                    var parallelJobs = prepareParallelScan(read, indexReadSession, indexCursor, propertyOffset);
                    if (parallelJobs != null) {
                        ParallelUtil.run(parallelJobs, executorService);
                        return;
                    }
                }
                // do a single threaded scan if:
                // feature flag was off, or concurrency is 1, or the thread pool is not usable, or we couldn't find a valid range
                var indexQueryConstraints = IndexQueryConstraints.unordered(true);
                read.nodeIndexScan(indexReadSession, indexCursor, indexQueryConstraints);
            }
            importFromCursor(indexCursor, propertyOffset);
        }
    }

    NodeLabel nodeLabel() {
        return nodeLabel;
    }

    PropertyMapping mapping() {
        return mapping;
    }

    long imported() {
        return imported;
    }

    NodePropertyValues build(IdMap idMap) {
        return propertiesBuilder.build(idMap);
    }

    private @Nullable List<IndexedNodePropertyImporter> prepareParallelScan(
        Read read,
        IndexReadSession indexReadSession,
        NodeValueIndexCursor indexCursor,
        int propertyOffset
    ) throws Exception {
        var anyValue = Neo4jProxy.rangeAllIndexQuery(this.propertyId);
        // find min value
        Neo4jProxy.nodeIndexSeek(read, indexReadSession, indexCursor, IndexOrder.ASCENDING, true, anyValue);
        var min = findFirst(indexCursor, propertyOffset);
        if (min.isPresent()) {
            // find max value
            Neo4jProxy.nodeIndexSeek(read, indexReadSession, indexCursor, IndexOrder.DESCENDING, true, anyValue);
            var max = findFirst(indexCursor, propertyOffset);
            if (max.isPresent()) {
                var minValue = min.getAsDouble();
                // nextUp to make the range exclusive
                var maxValue = Math.nextUp(max.getAsDouble());
                var range = maxValue - minValue;
                var batchSize = range / concurrency;
                // if min and max are too close together the batchSize could be small enough to not
                // change the value of minValue. In that case, increase it to guarantee that is always
                // has an effect.
                if (minValue == (minValue + batchSize)) {
                    batchSize = Math.nextUp(minValue) - minValue;
                }
                var jobs = new ArrayList<IndexedNodePropertyImporter>(this.concurrency);
                while (minValue < maxValue) {
                    var query = Neo4jProxy.rangeIndexQuery(
                        this.propertyId,
                        minValue,
                        true,
                        minValue + batchSize,
                        false
                    );
                    jobs.add(new IndexedNodePropertyImporter(this, query));
                    minValue += batchSize;
                }
                return jobs;
            }
        }
        return null;
    }

    private OptionalDouble findFirst(NodeValueIndexCursor indexCursor, int propertyOffset) {
        while (indexCursor.next()) {
            if (indexCursor.hasValue()) {
                var node = indexCursor.nodeReference();
                var nodeId = idMap.toMappedNodeId(node);
                if (nodeId >= 0) {
                    var propertyValue = indexCursor.propertyValue(propertyOffset);
                    var number = ((NumberValue) propertyValue).doubleValue();
                    if (Double.isFinite(number)) {
                        return OptionalDouble.of(number);
                    }
                }
            }
        }
        return OptionalDouble.empty();
    }

    private void importFromCursor(NodeValueIndexCursor indexCursor, int propertyOffset) {
        while (indexCursor.next()) {
            if (indexCursor.hasValue()) {
                var neoNodeId = indexCursor.nodeReference();
                var nodeId = idMap.toMappedNodeId(neoNodeId);
                if (nodeId >= 0) {
                    var propertyValue = indexCursor.propertyValue(propertyOffset);
                    propertiesBuilder.set(neoNodeId, propertyValue);
                    imported += 1;
                    if ((imported & 0x1_FFFFL) == 0L) {
                        progressTracker.logProgress(imported - logged);
                        logged = imported;
                        terminationFlag.assertRunning();
                    }
                }
            }
        }
    }
}
