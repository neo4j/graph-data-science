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

import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.logging.Log;

import java.util.Arrays;

import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;

public final class NodeScannerFactory {

    private NodeScannerFactory() {}

    public static StoreScanner.Factory<NodeReference> create(
        TransactionContext transactionContext,
        int[] labelIds,
        Log log
    ) {
        var hasNodeLabelIndex = hasNodeLabelIndex(transactionContext);

        if (!hasNodeLabelIndex && labelIds.length > 0) {
            log.info("[gds] Attempted to use node label index, but no index was found. Falling back to node store scan.");
        }

        if (Arrays.stream(labelIds).anyMatch(labelId -> labelId == ANY_LABEL) || !hasNodeLabelIndex) {
            return NodeCursorBasedScanner::new;
        } else if (labelIds.length == 1) {
            return (prefetchSize, transaction) -> new NodeLabelIndexBasedScanner(
                labelIds[0],
                prefetchSize,
                transaction,
                GdsFeatureToggles.USE_PARTITIONED_SCAN.isEnabled()
            );
        } else {
            return (prefetchSize, transaction) -> new MultipleNodeLabelIndexBasedScanner(
                labelIds,
                prefetchSize,
                transaction,
                GdsFeatureToggles.USE_PARTITIONED_SCAN.isEnabled()
            );
        }
    }

    private static boolean hasNodeLabelIndex(TransactionContext transactionContext) {
        return transactionContext.apply((tx, ktx) -> Neo4jProxy.hasNodeLabelIndex(ktx));
    }
}
