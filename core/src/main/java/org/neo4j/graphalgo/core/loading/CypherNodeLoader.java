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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.PropertyMapping;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

class CypherNodeLoader extends CypherRecordLoader<IdsAndProperties> {

    private final HugeLongArrayBuilder builder;
    private final NodeImporter importer;
    private final Map<org.neo4j.graphalgo.PropertyMapping, NodePropertiesBuilder> nodePropertyBuilders;
    private long maxNodeId;

    CypherNodeLoader(long nodeCount, GraphDatabaseAPI api, GraphSetup setup) {
        super(setup.startLabel, nodeCount, api, setup);
        maxNodeId = 0L;
        nodePropertyBuilders = nodeProperties(nodeCount, setup);
        builder = HugeLongArrayBuilder.of(nodeCount, setup.tracker);
        importer = new NodeImporter(builder, nodePropertyBuilders.values());
    }

    @Override
    BatchLoadResult loadOneBatch(long offset, int batchSize, int bufferSize) {
        NodesBatchBuffer buffer = new NodesBatchBuffer(null, -1, bufferSize, true);
        NodeRowVisitor visitor = new NodeRowVisitor(nodePropertyBuilders, buffer, importer);
        runLoadingQuery(offset, batchSize, visitor);
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
    IdsAndProperties result() {
        IdMap idMap = IdMapBuilder.build(builder, maxNodeId, setup.concurrency, setup.tracker);
        Map<String, PropertyMapping> nodeProperties = nodePropertyBuilders.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().propertyKey(), e -> e.getValue().build()));
        return new IdsAndProperties(idMap, nodeProperties);
    }

    private Map<org.neo4j.graphalgo.PropertyMapping, NodePropertiesBuilder> nodeProperties(long capacity, GraphSetup setup) {
        Map<org.neo4j.graphalgo.PropertyMapping, NodePropertiesBuilder> nodeProperties = new HashMap<>();
        for (org.neo4j.graphalgo.PropertyMapping propertyMapping : setup.nodePropertyMappings) {
            nodeProperties.put(
                    propertyMapping,
                    NodePropertiesBuilder.of(
                            capacity,
                            AllocationTracker.EMPTY,
                            propertyMapping.defaultValue(),
                            -2,
                            propertyMapping.propertyKey()));
        }
        return nodeProperties;
    }
}
