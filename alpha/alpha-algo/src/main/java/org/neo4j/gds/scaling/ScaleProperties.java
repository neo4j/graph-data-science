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
package org.neo4j.gds.scaling;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

/**
 * This algorithm takes as input a list of node property names and a scaler.
 * It applies the scaler to the node property values, respectively, and outputs a single node property.
 * The output node property values are lists of the same size as the input lists, and contain the scaled values
 * of the input node properties.
 */
public class ScaleProperties extends Algorithm<ScaleProperties, ScaleProperties.Result> {

    private final Graph graph;
    private final ScalePropertiesBaseConfig config;
    private final AllocationTracker tracker;
    private final ExecutorService executor;

    public ScaleProperties(
        Graph graph,
        ScalePropertiesBaseConfig config,
        AllocationTracker tracker,
        ExecutorService executor
    ) {
        this.graph = graph;
        this.config = config;
        this.tracker = tracker;
        this.executor = executor;
    }

    @Override
    public Result compute() {
        var scaledProperties = HugeObjectArray.newArray(double[].class, graph.nodeCount(), tracker);

        // Create a Scaler for each input property
        // Array properties are unrolled into multiple scalers
        var scalers = config.nodeProperties().stream()
            .map(this::prepareScalers)
            .collect(Collectors.toList());

        var outputArrayLength = scalers.stream().mapToInt(Scaler::dimension).sum();
        initializeArrays(scaledProperties, outputArrayLength);

        // Materialize each scaler and apply it to all properties
        var resultIndex = 0;
        for (var scaler : scalers) {
            scaleProperty(scaledProperties, scaler, resultIndex);
            resultIndex += scaler.dimension();
        }

        return Result.of(scaledProperties);
    }

    private void initializeArrays(HugeObjectArray<double[]> scaledProperties, int propertyCount) {
        var tasks = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            (partition) -> (Runnable) () -> partition.consume((nodeId) -> scaledProperties.set(
                nodeId,
                new double[propertyCount]
            ))
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
    }

    private void scaleProperty(HugeObjectArray<double[]> scaledProperties, Scaler scaler, int index) {
        if (scaler instanceof ScalarScaler) {
            var _scaler = (ScalarScaler) scaler;
            var tasks = PartitionUtils.rangePartition(
                config.concurrency(),
                graph.nodeCount(),
                (partition) -> (Runnable) () -> partition.consume((nodeId) -> {
                    var afterValue = _scaler.scaleProperty(nodeId);
                    double[] existingResult = scaledProperties.get(nodeId);
                    existingResult[index] = afterValue;
                })
            );
            ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
        } else {
            var tasks = PartitionUtils.rangePartition(
                config.concurrency(),
                graph.nodeCount(),
                (partition) -> (Runnable) () -> partition.consume((nodeId) -> {
                    scaler.scaleProperty(nodeId, scaledProperties.get(nodeId), index);
                })
            );
            ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
        }    }

    @Override
    public ScaleProperties me() {
        return this;
    }

    @Override
    public void release() {}

    @ValueClass
    interface Result {
        HugeObjectArray<double[]> scaledProperties();

        static Result of(HugeObjectArray<double[]> properties) {
            return ImmutableResult.of(properties);
        }
    }

    private Scaler prepareScalers(String propertyName) {
        var scalerVariant = config.scaler();
        var nodeProperties = graph.nodeProperties(propertyName);

        if (nodeProperties == null) {
            throw new IllegalArgumentException(formatWithLocale(
                "Node property `%s` not found in graph with node properties: %s",
                propertyName,
                graph.availableNodeProperties()
            ));
        }

        int arrayLength;
        List<Scaler> elementScalers;

        switch (nodeProperties.valueType()) {
            case LONG:
            case DOUBLE:
                return scalerVariant.create(
                    nodeProperties,
                    graph.nodeCount(),
                    config.concurrency(),
                    executor
                );
            case LONG_ARRAY:
                arrayLength = nodeProperties.longArrayValue(0).length;
                elementScalers = IntStream.range(0, arrayLength)
                    .mapToObj(idx -> scalerVariant.create(
                        transformLongArrayEntryToDoubleProperty(propertyName, nodeProperties, arrayLength, idx),
                        graph.nodeCount(),
                        config.concurrency(),
                        executor
                    )).collect(Collectors.toList());
                return new Scaler.ArrayScaler(elementScalers);
            case FLOAT_ARRAY:
                arrayLength = nodeProperties.floatArrayValue(0).length;
                elementScalers = IntStream.range(0, arrayLength)
                    .mapToObj(idx -> scalerVariant.create(
                        transformFloatArrayEntryToDoubleProperty(propertyName, nodeProperties, arrayLength, idx),
                        graph.nodeCount(),
                        config.concurrency(),
                        executor
                    )).collect(Collectors.toList());
                return new Scaler.ArrayScaler(elementScalers);
            case DOUBLE_ARRAY:
                arrayLength = nodeProperties.doubleArrayValue(0).length;
                elementScalers = IntStream.range(0, arrayLength)
                    .mapToObj(idx -> scalerVariant.create(
                        transformDoubleArrayEntryToDoubleProperty(propertyName, nodeProperties, arrayLength, idx),
                        graph.nodeCount(),
                        config.concurrency(),
                        executor
                    )).collect(Collectors.toList());
                return new Scaler.ArrayScaler(elementScalers);
            case UNKNOWN:
        }

        throw new UnsupportedOperationException(formatWithLocale(
            "Scaling node property `%s` of type `%s` is not supported",
            propertyName,
            nodeProperties.valueType().cypherName()
        ));
    }

    private DoubleNodeProperties transformFloatArrayEntryToDoubleProperty(String propertyName, NodeProperties property, int expectedArrayLength, int idx) {
        return (nodeId) -> {
            var propertyValue = property.floatArrayValue(nodeId);

            if (propertyValue == null || propertyValue.length != expectedArrayLength) {
                throw createInvalidArrayException(propertyName, expectedArrayLength, nodeId, Optional.ofNullable(propertyValue).map(v -> v.length).orElse(0));
            }
            return propertyValue[idx];
        };
    }

    private DoubleNodeProperties transformDoubleArrayEntryToDoubleProperty(String propertyName, NodeProperties property, int expectedArrayLength, int idx) {
        return (nodeId) -> {
            var propertyValue = property.doubleArrayValue(nodeId);

            if (propertyValue == null || propertyValue.length != expectedArrayLength) {
                throw createInvalidArrayException(propertyName, expectedArrayLength, nodeId, Optional.ofNullable(propertyValue).map(v -> v.length).orElse(0));
            }
            return propertyValue[idx];
        };
    }

    private DoubleNodeProperties transformLongArrayEntryToDoubleProperty(String propertyName, NodeProperties property, int expectedArrayLength, int idx) {
        return (nodeId) -> {
            var propertyValue = property.longArrayValue(nodeId);

            if (propertyValue == null || propertyValue.length != expectedArrayLength) {
                throw createInvalidArrayException(propertyName, expectedArrayLength, nodeId, Optional.ofNullable(propertyValue).map(v -> v.length).orElse(0));
            }
            return propertyValue[idx];
        };
    }

    private IllegalArgumentException createInvalidArrayException(
        String propertyName,
        int expectedArrayLength,
        long nodeId,
        int actualLength
    ) {
        return new IllegalArgumentException(formatWithLocale(
            "For scaling property `%s` expected array of length %d but got length %d for node %d",
            propertyName,
            expectedArrayLength,
            actualLength,
            nodeId
        ));
    }
}
