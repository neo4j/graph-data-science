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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;
import org.neo4j.values.storable.Value;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

public final class LazyIdMapBuilder implements PartialIdMap {
    private final AtomicBoolean isEmpty = new AtomicBoolean(true);
    private final ShardedLongLongMap.Builder intermediateIdMapBuilder;

    private final NodesBuilder nodesBuilder;

    public LazyIdMapBuilder(int concurrency, boolean hasLabelInformation, boolean hasProperties) {
        this.intermediateIdMapBuilder = ShardedLongLongMap.builder(concurrency);
        this.nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(concurrency)
            .maxOriginalId(NodesBuilder.UNKNOWN_MAX_ID)
            .hasLabelInformation(hasLabelInformation)
            .hasProperties(hasProperties)
            .deduplicateIds(false)
            .build();
    }

    public long addNode(long nodeId, @Nullable NodeLabelToken nodeLabels) {
        var intermediateId = this.intermediateIdMapBuilder.toMappedNodeId(nodeId);

        // deduplication
        if (intermediateId != IdMap.NOT_FOUND) {
            return intermediateId;
        }

        intermediateId = this.intermediateIdMapBuilder.addNode(nodeId);

        isEmpty.lazySet(false);
        if (nodeLabels == null) {
            nodeLabels = NodeLabelTokens.empty();
        }
        this.nodesBuilder.addNode(intermediateId, nodeLabels);

        return intermediateId;
    }

    public long addNodeWithProperties(
        long nodeId,
        Map<String, Value> properties,
        @Nullable NodeLabelToken nodeLabels
    ) {
        var intermediateId = this.intermediateIdMapBuilder.toMappedNodeId(nodeId);

        // deduplication
        if (intermediateId != IdMap.NOT_FOUND) {
            return intermediateId;
        }

        intermediateId = this.intermediateIdMapBuilder.addNode(nodeId);
        isEmpty.lazySet(false);
        if (nodeLabels == null) {
            nodeLabels = NodeLabelTokens.empty();
        }
        if (properties.isEmpty()) {
            this.nodesBuilder.addNode(intermediateId, nodeLabels);
        } else {
            this.nodesBuilder.addNode(intermediateId, properties, nodeLabels);
        }

        return intermediateId;
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return originalNodeId;
    }

    public long toIntermediateId(long originalId) {
        return this.intermediateIdMapBuilder.toMappedNodeId(originalId);
    }

    @Override
    public OptionalLong rootNodeCount() {
        return isEmpty.getAcquire()
            ? OptionalLong.empty()
            : OptionalLong.of(this.nodesBuilder.importedNodes());
    }

    @ValueClass
    public interface HighLimitIdMapAndProperties {
        HighLimitIdMap idMap();

        PartialIdMap intermediateIdMap();

        Optional<Map<String, NodePropertyValues>> nodeProperties();
    }

    public HighLimitIdMapAndProperties build() {
        var idMapAndProperties = this.nodesBuilder.build();
        var intermediateIdMap = this.intermediateIdMapBuilder.build();
        // The implementation of this map depends on either CE/EE or a feature toggle.
        var internalIdMap = idMapAndProperties.idMap();

        var idMap = new HighLimitIdMap(intermediateIdMap, internalIdMap);

        var partialIdMap = new PartialIdMap() {
            @Override
            public long toMappedNodeId(long intermediateId) {
                // This partial id map is used to construct the final node properties.
                // During import, the node properties are indexed by the intermediate id
                // produced by the LazyIdMap. To get the correct mapped id, we have to
                // go through the actual high limit id map.
                return idMap.toMappedNodeId(intermediateIdMap.toOriginalNodeId(intermediateId));
            }

            @Override
            public OptionalLong rootNodeCount() {
                return OptionalLong.of(intermediateIdMap.size());
            }
        };

        return ImmutableHighLimitIdMapAndProperties
            .builder()
            .idMap(idMap)
            .intermediateIdMap(partialIdMap)
            .nodeProperties(idMapAndProperties.nodeProperties())
            .build();
    }
}
