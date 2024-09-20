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
package org.neo4j.gds.projection;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.compat.CompatExecutionContext;
import org.neo4j.gds.compat.StoreScan;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.ArrayList;
import java.util.List;

public final class PartitionedStoreScan<C extends Cursor> implements StoreScan<C> {
    private final PartitionedScan<C> scan;

    public PartitionedStoreScan(PartitionedScan<C> scan) {
        this.scan = scan;
    }

    @Override
    public boolean reserveBatch(C cursor, CompatExecutionContext ctx) {
        return ctx.reservePartition(scan, cursor);
    }

    public static List<StoreScan<NodeLabelIndexCursor>> createScans(
        KernelTransaction transaction,
        int batchSize,
        int... labelIds
    ) {
        var indexDescriptor = NodeLabelIndexLookupImpl.findUsableMatchingIndex(
            transaction,
            SchemaDescriptors.forAnyEntityTokens(EntityType.NODE)
        );

        if (indexDescriptor == IndexDescriptor.NO_INDEX) {
            throw new IllegalStateException("There is no index that can back a node label scan.");
        }

        var read = transaction.dataRead();

        // Our current strategy is to select the token with the highest count
        // and use that one as the driving partitioned index scan. The partitions
        // of all other partitioned index scans will be aligned to that one.
        int maxToken = labelIds[0];
        long maxCount = read.estimateCountsForNode(labelIds[0]);
        int maxIndex = 0;

        for (int i = 1; i < labelIds.length; i++) {
            long count = read.estimateCountsForNode(labelIds[i]);
            if (count > maxCount) {
                maxCount = count;
                maxToken = labelIds[i];
                maxIndex = i;
            }
        }

        // swap the first label with the max count label
        labelIds[maxIndex] = labelIds[0];
        labelIds[0] = maxToken;

        int numberOfPartitions = PartitionedStoreScan.getNumberOfPartitions(maxCount, batchSize);

        try {
            var session = read.tokenReadSession(indexDescriptor);

            var partitionedScan = read.nodeLabelScan(
                session,
                numberOfPartitions,
                transaction.cursorContext(),
                new TokenPredicate(maxToken)
            );

            var scans = new ArrayList<StoreScan<NodeLabelIndexCursor>>(labelIds.length);
            scans.add(new PartitionedStoreScan<>(partitionedScan));

            // Initialize the remaining index scans with the partitioning of the first scan.
            for (int i = 1; i < labelIds.length; i++) {
                int labelToken = labelIds[i];
                var scan = read.nodeLabelScan(session, partitionedScan, new TokenPredicate(labelToken));
                scans.add(new PartitionedStoreScan<>(scan));
            }

            return scans;
        } catch (KernelException e) {
            // should not happen, we check for the index existence and applicability
            // before reading it
            throw new RuntimeException("Unexpected error while initialising reading from node label index", e);
        }
    }

    static int getNumberOfPartitions(long nodeCount, int batchSize) {
        int numberOfPartitions;
        if (nodeCount > 0) {
            // ceil div to try to get enough partitions so a single one does
            // not include more nodes than batchSize
            long partitions = ((nodeCount - 1) / batchSize) + 1;

            // value must be positive
            if (partitions < 1) {
                partitions = 1;
            }

            numberOfPartitions = (int) Long.min(Integer.MAX_VALUE, partitions);
        } else {
            // we have no partitions to scan, but the value must still  be positive
            numberOfPartitions = 1;
        }
        return numberOfPartitions;
    }
}
