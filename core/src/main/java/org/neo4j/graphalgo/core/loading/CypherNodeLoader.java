/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.LongHashSet;
import org.immutables.value.Value;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.PropertyMapping.DEFAULT_FALLBACK_VALUE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

@Value.Enclosing
class CypherNodeLoader extends CypherRecordLoader<CypherNodeLoader.LoadResult> {

    private static final int CYPHER_RESULT_PROPERTY_KEY = -2;

    private final long nodeCount;
    private final boolean hasExplicitPropertyMappings;
    private final GraphDimensions outerDimensions;

    private HugeLongArrayBuilder builder;
    private NodeImporter importer;
    private Map<PropertyMapping, NodePropertiesBuilder> nodePropertyBuilders;
    private long maxNodeId;
    private boolean initializedFromResult;

    CypherNodeLoader(
        String nodeQuery,
        long nodeCount,
        GraphDatabaseAPI api,
        GraphSetup setup,
        GraphDimensions outerDimensions
    ) {
        super(nodeQuery, nodeCount, api, setup);
        this.nodeCount = nodeCount;
        this.outerDimensions = outerDimensions;
        this.maxNodeId = 0L;
        this.hasExplicitPropertyMappings = setup.nodePropertyMappings().hasMappings();

        if (hasExplicitPropertyMappings) {
            initImporter(setup.nodePropertyMappings());
        }
    }

    private void initImporter(PropertyMappings nodeProperties) {
        nodePropertyBuilders = nodeProperties(nodeProperties);
        builder = HugeLongArrayBuilder.of(nodeCount, setup.tracker());
        importer = new NodeImporter(builder, nodePropertyBuilders.values());
    }

    @Override
    BatchLoadResult loadOneBatch(long offset, int batchSize, int bufferSize) {
        Result result = runLoadingQuery(offset, batchSize);

        if (!hasExplicitPropertyMappings && !initializedFromResult) {
            // init from columns
            Collection<String> propertyColumns = getPropertyColumns(result);

            PropertyMappings propertyMappings = PropertyMappings.of(propertyColumns
                .stream()
                .map(propertyColumn -> PropertyMapping.of(
                    propertyColumn,
                    propertyColumn,
                    DEFAULT_FALLBACK_VALUE,
                    Aggregation.DEFAULT
                ))
                .toArray(PropertyMapping[]::new));

            initImporter(propertyMappings);
            initializedFromResult = true;
        }

        NodesBatchBuffer buffer = new NodesBatchBuffer(null, new LongHashSet(), bufferSize, true);
        NodeRowVisitor visitor = new NodeRowVisitor(nodePropertyBuilders, buffer, importer);
        result.accept(visitor);
        visitor.flush();
        return new BatchLoadResult(offset, visitor.rows(), visitor.maxId(), visitor.rows());
    }

    @Override
    void updateCounts(BatchLoadResult result) {
        if (result.maxId() > maxNodeId) {
            maxNodeId = result.maxId();
        }
    }

    @Override
    LoadResult result() {
        IdMap idMap = IdMapBuilder.build(builder, maxNodeId, setup.concurrency(), setup.tracker());
        Map<String, NodeProperties> nodeProperties = nodePropertyBuilders.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().propertyKey(), e -> e.getValue().build()));

        ResolvedPropertyMappings nodePropertyMappings = ResolvedPropertyMappings.of(
            nodePropertyBuilders
                .keySet()
                .stream()
                .map(x -> x.resolveWith(NO_SUCH_PROPERTY_KEY))
                .collect(Collectors.toList())
        );

        GraphDimensions resultDimensions = ImmutableGraphDimensions.builder()
            .from(outerDimensions)
            .nodeProperties(nodePropertyMappings)
            .build();

        return ImmutableCypherNodeLoader.LoadResult.builder()
            .dimensions(resultDimensions)
            .idsAndProperties(new IdsAndProperties(idMap, nodeProperties))
            .build();
    }

    @Override
    Set<String> getReservedColumns() {
        return NodeRowVisitor.RESERVED_COLUMNS;
    }

    private Map<PropertyMapping, NodePropertiesBuilder> nodeProperties(PropertyMappings propertyMappings) {
        return propertyMappings.stream().collect(Collectors.toMap(
            propertyMapping -> propertyMapping,
            propertyMapping -> NodePropertiesBuilder.of(
                nodeCount,
                AllocationTracker.EMPTY,
                propertyMapping.defaultValue(),
                CYPHER_RESULT_PROPERTY_KEY,
                propertyMapping.propertyKey()
            )
        ));
    }

    @ValueClass
    interface LoadResult {
        GraphDimensions dimensions();

        IdsAndProperties idsAndProperties();
    }
}
