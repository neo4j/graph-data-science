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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateFromCypherConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.graphdb.Transaction;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

@Value.Enclosing
class CypherNodeLoader extends CypherRecordLoader<CypherNodeLoader.LoadResult> {

    private final long nodeCount;
    private final GraphDimensions outerDimensions;

    private long highestNodeId;
    private NodesBuilder nodesBuilder;

    CypherNodeLoader(
        String nodeQuery,
        long nodeCount,
        GraphCreateFromCypherConfig config,
        GraphLoaderContext loadingContext,
        GraphDimensions outerDimensions
    ) {
        super(nodeQuery, nodeCount, config, loadingContext);
        this.nodeCount = nodeCount;
        this.outerDimensions = outerDimensions;
        this.highestNodeId = 0L;
    }

    @Override
    BatchLoadResult loadSingleBatch(Transaction tx, int bufferSize) {
        var queryResult = runLoadingQuery(tx);

        var propertyColumns = getPropertyColumns(queryResult);

        var hasLabelInformation = queryResult.columns().contains(NodeRowVisitor.LABELS_COLUMN);
        nodesBuilder = GraphFactory.initNodesBuilder()
            .nodeCount(nodeCount)
            .maxOriginalId(NodesBuilder.UNKNOWN_MAX_ID)
            .hasLabelInformation(hasLabelInformation)
            .hasProperties(!propertyColumns.isEmpty())
            .tracker(loadingContext.tracker())
            .build();

        NodeRowVisitor visitor = new NodeRowVisitor(nodesBuilder, propertyColumns, hasLabelInformation);

        queryResult.accept(visitor);
        long rows = visitor.rows();
        if (rows == 0) {
            throw new IllegalArgumentException("Node-Query returned no nodes");
        }
        return new BatchLoadResult(rows, visitor.maxId());
    }

    @Override
    void updateCounts(BatchLoadResult result) {
        if (result.maxId() > highestNodeId) {
            highestNodeId = result.maxId();
        }
    }

    @Override
    LoadResult result() {
        var nodeMappingAndProperties = nodesBuilder.buildChecked(highestNodeId);
        var nodeMapping = nodeMappingAndProperties.nodeMapping();
        var nodeProperties = nodeMappingAndProperties.nodeProperties().orElseGet(Map::of);
        var nodePropertiesWithPropertyMappings = propertiesWithPropertyMappings(nodeProperties);

        Map<String, Integer> propertyIds = nodePropertiesWithPropertyMappings
            .values()
            .stream()
            .flatMap(properties -> properties.keySet().stream())
            .distinct()
            .collect(Collectors.toMap(PropertyMapping::propertyKey, (ignore) -> NO_SUCH_PROPERTY_KEY));

        GraphDimensions resultDimensions = ImmutableGraphDimensions.builder()
            .from(outerDimensions)
            .nodePropertyTokens(propertyIds)
            .build();

        return ImmutableCypherNodeLoader.LoadResult.builder()
            .dimensions(resultDimensions)
            .idsAndProperties(IdsAndProperties.of(nodeMapping, nodePropertiesWithPropertyMappings))
            .build();
    }

    @Override
    Set<String> getMandatoryColumns() {
        return NodeRowVisitor.REQUIRED_COLUMNS;
    }

    @Override
    Set<String> getReservedColumns() {
        return NodeRowVisitor.RESERVED_COLUMNS;
    }

    @Override
    QueryType queryType() {
        return QueryType.NODE;
    }

    private static Map<NodeLabel, Map<PropertyMapping, NodeProperties>> propertiesWithPropertyMappings(Map<NodeLabel, Map<String, NodeProperties>> properties) {
        return properties
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().entrySet().stream().collect(Collectors.toMap(
                    propertiesByKey -> PropertyMapping.of(propertiesByKey.getKey(), propertiesByKey.getValue().valueType().fallbackValue()),
                    Map.Entry::getValue
                ))
            ));
    }

    @ValueClass
    interface LoadResult {
        GraphDimensions dimensions();

        IdsAndProperties idsAndProperties();
    }
}
