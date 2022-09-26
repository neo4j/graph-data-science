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

import org.immutables.value.Value;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Value.Enclosing
class CypherNodeLoader extends CypherRecordLoader<IdMapAndProperties> {

    private final long nodeCount;
    private final ProgressTracker progressTracker;

    private long highestNodeId;
    private NodesBuilder nodesBuilder;

    CypherNodeLoader(
        String nodeQuery,
        long nodeCount,
        GraphProjectFromCypherConfig config,
        GraphLoaderContext loadingContext,
        ProgressTracker progressTracker
    ) {
        super(nodeQuery, nodeCount, config, loadingContext);
        this.nodeCount = nodeCount;
        this.progressTracker = progressTracker;
        this.highestNodeId = 0L;
    }

    @Override
    BatchLoadResult loadSingleBatch(InternalTransaction tx, int bufferSize) {
        progressTracker.beginSubTask("Nodes");
        progressTracker.setVolume(nodeCount);

        var nodeSubscriber = new NodeSubscriber(progressTracker);
        var subscription = runLoadingQuery(tx, nodeSubscriber);
        var propertyColumns = getPropertyColumns(subscription);
        var hasLabelInformation = columnsContains(subscription, NodeSubscriber.LABELS_COLUMN);

        this.nodesBuilder = GraphFactory.initNodesBuilder()
            .nodeCount(nodeCount)
            .maxOriginalId(NodesBuilder.UNKNOWN_MAX_ID)
            .hasLabelInformation(hasLabelInformation)
            .hasProperties(!propertyColumns.isEmpty())
            .build();

        nodeSubscriber.initialize(subscription.fieldNames(), this.nodesBuilder);
        try {
            CypherLoadingUtils.consume(subscription);
        } catch (RuntimeException e) {
            nodesBuilder.close(e);
        }

        nodeSubscriber.error().ifPresent(error -> nodesBuilder.close(error));

        long rows = nodeSubscriber.rows();
        if (rows == 0) {
            nodesBuilder.close(new IllegalArgumentException("Node-Query returned no nodes"));
        }
        progressTracker.endSubTask("Nodes");
        return new BatchLoadResult(rows, nodeSubscriber.maxId());
    }

    @Override
    void updateCounts(BatchLoadResult result) {
        if (result.maxId() > highestNodeId) {
            highestNodeId = result.maxId();
        }
    }

    @Override
    IdMapAndProperties result() {
        var idMapAndProperties = nodesBuilder.build(highestNodeId);
        var idMap = idMapAndProperties.idMap();
        var nodeProperties = idMapAndProperties.nodeProperties().orElseGet(Map::of);
        var nodePropertiesWithPropertyMappings = propertiesWithPropertyMappings(nodeProperties);

        return IdMapAndProperties.of(idMap, nodePropertiesWithPropertyMappings);
    }

    @Override
    Set<String> getMandatoryColumns() {
        return NodeSubscriber.REQUIRED_COLUMNS;
    }

    @Override
    Set<String> getReservedColumns() {
        return NodeSubscriber.RESERVED_COLUMNS;
    }

    @Override
    QueryType queryType() {
        return QueryType.NODE;
    }

    private static Map<PropertyMapping, NodePropertyValues> propertiesWithPropertyMappings(Map<String, NodePropertyValues> properties) {
        return properties.entrySet()
            .stream()
            .collect(Collectors.toMap(
                propertiesByKey -> PropertyMapping.of(
                    propertiesByKey.getKey(),
                    propertiesByKey.getValue().valueType().fallbackValue()
                ),
                Map.Entry::getValue
            ));
    }
}
