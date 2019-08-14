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
package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.huge.loader.HugeIdMapBuilder;
import org.neo4j.graphalgo.core.huge.loader.HugeNodePropertiesBuilder;
import org.neo4j.graphalgo.core.huge.loader.IdMap;
import org.neo4j.graphalgo.core.huge.loader.IdsAndProperties;
import org.neo4j.graphalgo.core.huge.loader.NodeImporter;
import org.neo4j.graphalgo.core.huge.loader.NodesBatchBuffer;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

class CypherNodeLoader {

    private final GraphDatabaseAPI api;
    private final GraphSetup setup;
    private HugeLongArrayBuilder builder;
    private NodeImporter importer;
    private Map<PropertyMapping, HugeNodePropertiesBuilder> nodePropertyBuilders;

    public CypherNodeLoader(GraphDatabaseAPI api, GraphSetup setup) {
        this.api = api;
        this.setup = setup;
    }

    public IdsAndProperties load(long nodeCount) {
        int batchSize = setup.batchSize;
        this.nodePropertyBuilders = nodeProperties(nodeCount);
        this.builder = HugeLongArrayBuilder.of(nodeCount, setup.tracker);
        this.importer = new NodeImporter(builder, nodePropertyBuilders.values());
        return CypherLoadingUtils.canBatchLoad(setup.loadConcurrent(), batchSize, setup.startLabel) ?
                parallelLoadNodes(batchSize) :
                nonParallelLoadNodes();
    }

    private IdsAndProperties parallelLoadNodes(int batchSize) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();

        long offset = 0;
        long lastOffset = 0;
        long maxNodeId = 0;
        List<Future<ImportState>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            futures.add(pool.submit(() -> loadNodes(skip, batchSize, true)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<ImportState> future : futures) {
                    ImportState result = CypherLoadingUtils.get("Error during loading nodes offset: " + (lastOffset + batchSize), future);
                    lastOffset = result.offset();
                    working = result.rows() > 0;
                    maxNodeId = Math.max(maxNodeId, result.maxId());
                }
                futures.clear();
            }
        } while (working);

        IdMap idMap = HugeIdMapBuilder.build(builder, maxNodeId, setup.concurrency, setup.tracker);
        Map<String, HugeWeightMapping> nodeProperties = nodePropertyBuilders.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().propertyName, e -> e.getValue().build()));
        return new IdsAndProperties(idMap, nodeProperties);
    }

    private IdsAndProperties nonParallelLoadNodes() {
        ImportState nodes = loadNodes(0L, ParallelUtil.DEFAULT_BATCH_SIZE, false);
        IdMap idMap = HugeIdMapBuilder.build(builder, nodes.maxId(), setup.concurrency, setup.tracker);
        Map<String, HugeWeightMapping> nodeProperties = nodePropertyBuilders.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().propertyName, e -> e.getValue().build()));
        return new IdsAndProperties(idMap, nodeProperties);
    }

    private ImportState loadNodes(long offset, int batchSize, boolean withPaging) {
        NodesBatchBuffer buffer = new NodesBatchBuffer(null, -1, batchSize, true);

        NodeRowVisitor visitor = new NodeRowVisitor(nodePropertyBuilders, buffer, importer);
        Map<String, Object> parameters = withPaging
                ? CypherLoadingUtils.params(setup.params, offset, batchSize)
                : setup.params;
        api.execute(setup.startLabel, parameters).accept(visitor);
        visitor.flush();
        return new ImportState(offset, visitor.rows(), visitor.maxId(), visitor.rows());
    }

    private Map<PropertyMapping, HugeNodePropertiesBuilder> nodeProperties(long capacity) {
        Map<PropertyMapping, HugeNodePropertiesBuilder> nodeProperties = new HashMap<>();
        for (PropertyMapping propertyMapping : setup.nodePropertyMappings) {
            nodeProperties.put(
                    propertyMapping,
                    HugeNodePropertiesBuilder.of(
                            capacity,
                            AllocationTracker.EMPTY,
                            propertyMapping.defaultValue,
                            -2,
                            propertyMapping.propertyName));
        }
        return nodeProperties;
    }

}
