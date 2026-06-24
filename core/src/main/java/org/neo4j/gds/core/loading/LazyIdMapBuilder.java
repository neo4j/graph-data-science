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

import org.immutables.builder.Builder;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.ToMappedNodeId;
import org.neo4j.gds.api.nodes.ComposedIdMap;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.schema.NodeSchemaRecord;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.PropertyValues;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

public final class LazyIdMapBuilder implements PartialIdMap {
    private final AtomicBoolean isEmpty = new AtomicBoolean(true);
    private final ShardedLongLongMap.Builder intermediateIdMapBuilder;

    private final NodesBuilder nodesBuilder;

    @Builder.Constructor
    public LazyIdMapBuilder(
        Concurrency concurrency,
        Optional<Boolean> hasLabelInformation,
        Optional<Boolean> hasProperties,
        Optional<Boolean> usePooledLocalNodesBuilder,
        PropertyState propertyState
    ) {
        this.intermediateIdMapBuilder = ShardedLongLongMap.builder(concurrency);
        this.nodesBuilder = GraphFactory.initNodesBuilder()
            // The inner builder's label/property output is keyed by its inner mapped id, while
            // the final ShardedIdMap exposes the intermediate id as the mapped id. Only an
            // identity inner builder keeps those two id spaces aligned; a reordering builder
            // (e.g. the array-based default under concurrency) would mis-attribute labels and
            // properties to the wrong nodes.
            .idMapBuilderType(IdentityIdMap.Builder.ID)
            .concurrency(concurrency)
            .hasLabelInformation(hasLabelInformation)
            .hasProperties(hasProperties)
            .deduplicateIds(false)
            .usePooledBuilderProvider(usePooledLocalNodesBuilder)
            .propertyState(propertyState)
            .build();
    }

    public void prepareForFlush() {
        isEmpty.set(false);
    }

    public long addNode(long nodeId, NodeLabelToken nodeLabels) {
        LoadingExceptions.checkPositiveId(nodeId);

        long intermediateId = this.intermediateIdMapBuilder.addNode(nodeId);

        // deduplication
        if (intermediateId < 0) {
            return -(intermediateId + 1);
        }

        this.nodesBuilder.addNode(intermediateId, nodeLabels);

        return intermediateId;
    }

    public long addNodeWithProperties(
        long nodeId,
        PropertyValues properties,
        NodeLabelToken nodeLabels
    ) {
        long intermediateId = this.intermediateIdMapBuilder.addNode(nodeId);

        LoadingExceptions.checkPositiveId(nodeId);

        // deduplication
        if (intermediateId < 0) {
            return -(intermediateId + 1);
        }

        if (properties.isEmpty()) {
            this.nodesBuilder.addNode(intermediateId, nodeLabels);
        } else {
            this.nodesBuilder.addNode(intermediateId, nodeLabels, properties);
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

    public record ShardedIdMapAndProperties(
        IdMap idMap,
        NodeSchemaRecord schema,
        NodePropertyStore propertyStore
    ) {
        // The inner builder is forced to identity (see constructor), so the inner id map maps
        // the dense intermediate ids to themselves: exactly the identity intermediate -> mapped
        // id translation that node-property finalization and relationship value mapping require.
        public ToMappedNodeId toMappedNodeId() {
            return id -> id;
        }
    }

    public ShardedIdMapAndProperties build() {
        var nodes = this.nodesBuilder.build();
        var shardedIdMap = this.intermediateIdMapBuilder.build();

        // The inner builder is forced to identity (see constructor), so its mapped ids equal the
        // intermediate ids that ShardedIdMap exposes and label/property keying lines up. Guard the
        // invariant defensively in case that ever changes.
        if (nodes.idMap().nodeCount() != shardedIdMap.size()) {
            throw new IllegalStateException(
                "ShardedIdMap requires inner mapped ids to equal intermediate ids: " +
                "inner node count = " + nodes.idMap().nodeCount() +
                ", intermediate map size = " + shardedIdMap.size()
            );
        }

        var identityIdMap = nodes.idMap();
        var composedIdMap = ComposedIdMap.of(new ShardedIdMap(shardedIdMap), identityIdMap.labelInformation());

        return new ShardedIdMapAndProperties(
            composedIdMap,
            nodes.schema(),
            nodes.properties()
        );
    }
}
