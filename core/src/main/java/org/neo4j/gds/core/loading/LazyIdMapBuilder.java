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
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.ImmutableIdMapAndProperties;
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;
import org.neo4j.values.storable.Value;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.neo4j.gds.config.ConcurrencyConfig.DEFAULT_CONCURRENCY;

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

    @Override
    public OptionalLong rootNodeCount() {
        return isEmpty.getAcquire()
            ? OptionalLong.empty()
            : OptionalLong.of(this.nodesBuilder.importedNodes());
    }

    public NodesBuilder.IdMapAndProperties build() {
        var idMapAndProperties = this.nodesBuilder.build();
        var intermediateIdMap = this.intermediateIdMapBuilder.build();
        var internalIdMap = idMapAndProperties.idMap();

        var idMap = new HighLimitIdMap(intermediateIdMap, internalIdMap);

        return ImmutableIdMapAndProperties
            .builder()
            .idMap(idMap)
            .nodeProperties(idMapAndProperties.nodeProperties())
            .build();
    }
}
