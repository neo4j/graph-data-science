/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;

import java.util.Collection;
import java.util.Collections;

final class NodesScanner extends StatementAction implements RecordScanner {

    static ImportingThreadPool.CreateScanner of(
            GraphDatabaseAPI api,
            AbstractStorePageCacheScanner<NodeRecord> scanner,
            int label,
            ImportProgress progress,
            HugeLongArrayBuilder idMapBuilder,
            Collection<HugeNodePropertiesBuilder> nodePropertyBuilders) {
        return new NodesScanner.Creator(
                api,
                scanner,
                label,
                progress,
                idMapBuilder,
                nodePropertyBuilders
        );
    }

    static final class Creator implements ImportingThreadPool.CreateScanner {
        private final GraphDatabaseAPI api;
        private final AbstractStorePageCacheScanner<NodeRecord> scanner;
        private final int label;
        private final ImportProgress progress;
        private final HugeLongArrayBuilder idMapBuilder;
        private final IntObjectMap<HugeNodePropertiesBuilder> nodePropertyBuilders;

        Creator(
                GraphDatabaseAPI api,
                AbstractStorePageCacheScanner<NodeRecord> scanner,
                int label,
                ImportProgress progress,
                HugeLongArrayBuilder idMapBuilder,
                Collection<HugeNodePropertiesBuilder> nodePropertyBuilders) {
            this.api = api;
            this.scanner = scanner;
            this.label = label;
            this.progress = progress;
            this.idMapBuilder = idMapBuilder;
            this.nodePropertyBuilders = mapBuilders(nodePropertyBuilders);
        }

        @Override
        public RecordScanner create(final int index) {
            return new NodesScanner(
                    api,
                    scanner,
                    label,
                    index,
                    progress,
                    idMapBuilder,
                    nodePropertyBuilders
            );
        }

        @Override
        public Collection<Runnable> flushTasks() {
            return Collections.emptyList();
        }

        private IntObjectMap<HugeNodePropertiesBuilder> mapBuilders(Collection<HugeNodePropertiesBuilder> builders) {
            if (builders == null || builders.isEmpty()) {
                return null;
            }
            IntObjectMap<HugeNodePropertiesBuilder> map = new IntObjectHashMap<>(builders.size());
            for (HugeNodePropertiesBuilder builder : builders) {
                map.put(builder.propertyId(), builder);
            }
            return map;
        }
    }

    private final NodeStore nodeStore;
    private final AbstractStorePageCacheScanner<NodeRecord> scanner;
    private final int label;
    private final int scannerIndex;
    private final ImportProgress progress;
    private final HugeLongArrayBuilder idMapBuilder;
    private final IntObjectMap<HugeNodePropertiesBuilder> nodePropertyBuilders;
    private long nodePropertiesRead = 0L;

    private volatile long relationshipsImported;

    private NodesScanner(
            GraphDatabaseAPI api,
            AbstractStorePageCacheScanner<NodeRecord> scanner,
            int label,
            int threadIndex,
            ImportProgress progress,
            HugeLongArrayBuilder idMapBuilder,
            IntObjectMap<HugeNodePropertiesBuilder> nodePropertyBuilders) {
        super(api);
        this.nodeStore = (NodeStore) scanner.store();
        this.scanner = scanner;
        this.label = label;
        this.scannerIndex = threadIndex;
        this.progress = progress;
        this.idMapBuilder = idMapBuilder;
        this.nodePropertyBuilders = nodePropertyBuilders;
    }

    @Override
    public String threadName() {
        return "node-store-scan-" + scannerIndex;
    }

    @Override
    public void accept(final KernelTransaction transaction) {
        Read read = transaction.dataRead();
        CursorFactory cursors = transaction.cursors();
        try (AbstractStorePageCacheScanner<NodeRecord>.Cursor cursor = scanner.getCursor()) {
            NodesBatchBuffer batches = new NodesBatchBuffer(
                    nodeStore,
                    label,
                    cursor.bulkSize(),
                    nodePropertyBuilders != null);
            final ImportProgress progress = this.progress;
            long allImported = 0L;
            while (batches.scan(cursor)) {
                int imported = importNodes(batches, read, cursors);
                progress.relationshipsImported(imported);
                allImported += imported;
            }
            relationshipsImported = allImported;
        }
    }

    @Override
    public long propertiesImported() {
        return nodePropertiesRead;
    }

    @Override
    public long recordsImported() {
        return relationshipsImported;
    }

    private int importNodes(
            final NodesBatchBuffer buffer,
            final Read read,
            final CursorFactory cursors) {

        int batchLength = buffer.length();
        if (batchLength == 0) {
            return 0;
        }

        HugeLongArrayBuilder.BulkAdder<long[]> adder = idMapBuilder.allocate((long) (batchLength));
        if (adder == null) {
            return 0;
        }

        long[] batch = buffer.batch();
        long[] properties = buffer.properties();
        int batchOffset = 0;
        while (adder.nextBuffer()) {
            int length = adder.length;
            System.arraycopy(batch, batchOffset, adder.buffer, adder.offset, length);

            if (properties != null) {
                long start = adder.start;
                for (int i = 0; i < length; i++) {
                    long localIndex = start + i;
                    int batchIndex = batchOffset + i;
                    readWeight(
                            batch[batchIndex],
                            properties[batchIndex],
                            nodePropertyBuilders,
                            localIndex,
                            cursors,
                            read
                    );
                }
            }

            batchOffset += length;
        }

        return batchLength;
    }

    private void readWeight(
            long nodeReference,
            long propertiesReference,
            IntObjectMap<HugeNodePropertiesBuilder> nodeProperties,
            long internalId,
            CursorFactory cursors,
            Read read) {
        try (PropertyCursor pc = cursors.allocatePropertyCursor()) {
            read.nodeProperties(nodeReference, propertiesReference, pc);
            while (pc.next()) {
                HugeNodePropertiesBuilder props = nodeProperties.get(pc.propertyKey());
                if (props != null) {
                    Value value = pc.propertyValue();
                    double defaultValue = props.defaultValue();
                    double weight = ReadHelper.extractValue(value, defaultValue);
                    props.set(internalId, weight);
                    nodePropertiesRead++;
                }
            }
        }
    }
}
