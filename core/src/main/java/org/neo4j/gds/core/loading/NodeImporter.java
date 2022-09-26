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

import com.carrotsearch.hppc.IntObjectMap;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.gds.core.utils.RawValues;

import java.util.Collections;
import java.util.List;

public class NodeImporter {

    public interface PropertyReader {
        int readProperty(long nodeReference, long[] labelIds, PropertyReference propertiesReference);
    }

    private final IdMapBuilder idMapBuilder;
    private final LabelInformation.Builder labelInformationBuilder;
    private final IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping;
    private final boolean importProperties;

    public NodeImporter(
        IdMapBuilder idMapBuilder,
        LabelInformation.Builder labelInformationBuilder,
        IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping,
        boolean importProperties
    ) {
        this.idMapBuilder = idMapBuilder;
        this.labelInformationBuilder = labelInformationBuilder;
        this.labelTokenNodeLabelMapping = labelTokenNodeLabelMapping;
        this.importProperties = importProperties;
    }

    public long importNodes(NodesBatchBuffer buffer, PropertyReader reader) {
        int batchLength = buffer.length();
        if (batchLength == 0) {
            return 0;
        }

        IdMapAllocator idMapAllocator = idMapBuilder.allocate(batchLength);

        //  Since we read the graph size in one transaction and load in multiple
        //  different transactions, any new data that is being added during loading
        //  will show up while scanning, but would not be accounted for when
        //  sizing the data structures used for loading.
        //
        //  The node loading part only accepts nodes that are within the
        //  calculated capacity that we have available.
        batchLength = idMapAllocator.allocatedSize();

        if (batchLength == 0) {
            return 0;
        }

        var batch = buffer.batch();
        var properties = buffer.properties();
        var labelIds = buffer.labelIds();

        // Import node IDs
        idMapAllocator.insert(batch);

        // Import node labels
        if (buffer.hasLabelInformation()) {
            setNodeLabelInformation(
                batch,
                batchLength,
                labelIds,
                (nodeIds, pos) -> nodeIds[pos]
            );
        }

        // Import node properties
        var importedProperties = importProperties
            ? importProperties(reader, batch, properties, labelIds, batchLength)
            : 0;

        return RawValues.combineIntInt(batchLength, importedProperties);
    }

    private void setNodeLabelInformation(long[] batch, int batchLength, long[][] labelIds, IdFunction idFunction) {
        int cappedBatchLength = Math.min(labelIds.length, batchLength);
        for (int i = 0; i < cappedBatchLength; i++) {
            long nodeId = idFunction.apply(batch, i);
            long[] labelIdsForNode = labelIds[i];

            for (long labelId : labelIdsForNode) {
                var elementIdentifiers = labelTokenNodeLabelMapping.getOrDefault(
                    (int) labelId,
                    Collections.emptyList()
                );
                for (NodeLabel elementIdentifier : elementIdentifiers) {
                    labelInformationBuilder.addNodeIdToLabel(elementIdentifier, nodeId);
                }
            }
        }
    }

    private static int importProperties(
        NodeImporter.PropertyReader reader,
        long[] batch,
        PropertyReference[] properties,
        long[][] labelIds,
        int length
    ) {
        int batchImportedProperties = 0;
        for (int i = 0; i < length; i++) {
            batchImportedProperties += reader.readProperty(
                batch[i],
                labelIds[i],
                properties[i]
            );
        }
        return batchImportedProperties;
    }

    @FunctionalInterface
    interface IdFunction {
        long apply(long[] batch, int pos);
    }
}
