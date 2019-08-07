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

import org.neo4j.graphalgo.KernelPropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * @author mknblch
 */
public class HeavyGraphFactory extends GraphFactory {

    public HeavyGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public Graph importGraph() {
        return importGraph(setup.batchSize);
    }

    @Override
    public final MemoryEstimation memoryEstimation() {
        return getMemoryEstimation(setup, dimensions);
    }

    public static MemoryEstimation getMemoryEstimation(final GraphSetup setup, final GraphDimensions dimensions) {
        MemoryEstimations.Builder builder = MemoryEstimations
                .builder(HeavyGraph.class)
                .add("nodeIdMap", IntIdMap.memoryEstimation())
                .add("container", AdjacencyMatrix.memoryEstimation(
                        setup.loadIncoming,
                        setup.loadOutgoing,
                        setup.loadAsUndirected,
                        dimensions.relWeightId() != StatementConstants.NO_SUCH_PROPERTY_KEY
                ))
                .startField("nodePropertiesMapping", Map.class);

        for (KernelPropertyMapping nodeProperty : dimensions.nodeProperties()) {
            if (nodeProperty.propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY) {
                builder.add(NullWeightMap.MEMORY_USAGE);
            } else {
                builder.add(WeightMap.memoryEstimation());
            }
        }

        return builder.endField().build();
    }

    private Graph importGraph(final int batchSize) {
        final IntIdMap idMap = loadIdMap();

        Map<String, Supplier<WeightMapping>> nodePropertySuppliers = new HashMap<>();
        for (KernelPropertyMapping propertyMapping : dimensions.nodeProperties()) {
            nodePropertySuppliers.put(
                    propertyMapping.propertyName,
                    () -> newWeightMap(
                            propertyMapping.propertyKeyId,
                            propertyMapping.defaultValue
                    )
            );
        }

        int concurrency = setup.concurrency();
        final int nodeCount = dimensions.nodeCountAsInt();
        final AdjacencyMatrix matrix = new AdjacencyMatrix(
                nodeCount,
                setup.loadIncoming && !setup.loadAsUndirected,
                setup.loadOutgoing || setup.loadAsUndirected,
                dimensions.relWeightId() != StatementConstants.NO_SUCH_PROPERTY_KEY,
                setup.relationDefaultWeight,
                setup.sort || setup.loadAsUndirected,
                false,
                setup.tracker);
        int actualBatchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                batchSize);
        AtomicLong relationshipCounter = new AtomicLong();
        Collection<RelationshipImporter> tasks = ParallelUtil.readParallel(
                concurrency,
                actualBatchSize,
                idMap,
                (offset, nodeIds) -> new RelationshipImporter(
                        api,
                        setup,
                        dimensions,
                        progress,
                        actualBatchSize,
                        offset,
                        idMap,
                        matrix,
                        nodeIds,
                        nodePropertySuppliers,
                        relationshipCounter
                ),
                threadPool);

        final Graph graph = buildCompleteGraph(
                matrix,
                idMap,
                nodePropertySuppliers,
                relationshipCounter.get(),
                tasks);

        progressLogger.logDone(setup.tracker);
        return graph;
    }

    private Graph buildCompleteGraph(
            final AdjacencyMatrix matrix,
            final IntIdMap idMap,
            final Map<String, Supplier<WeightMapping>> nodePropertySuppliers,
            final long relationshipCount,
            Collection<RelationshipImporter> tasks) {
        if (tasks.size() == 1) {
            RelationshipImporter importer = tasks.iterator().next();
            final Graph graph = importer.toGraph(idMap, matrix, relationshipCount);
            importer.release();
            return graph;
        }

        Map<String, WeightMapping> nodeProperties = new HashMap<>();
        nodePropertySuppliers.forEach((key, value) -> nodeProperties.put(key, value.get()));

        for (RelationshipImporter task : tasks) {
            task.writeInto(nodeProperties);
            task.release();
        }

        return new HeavyGraph(
                idMap,
                matrix,
                relationshipCount,
                nodeProperties);
    }

    private WeightMapping newWeightMap(int propertyId, double defaultValue) {
        return propertyId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(defaultValue)
                : new WeightMap(dimensions.nodeCountAsInt(), defaultValue, propertyId);
    }
}
