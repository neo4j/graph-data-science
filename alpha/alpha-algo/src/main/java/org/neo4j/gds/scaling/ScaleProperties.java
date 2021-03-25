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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

/**
 * This algorithm takes as input a list of node property names and a same-sized list of scalers.
 * It applies the scalers to the node property values, respectively, and outputs a single node property.
 * The output node property values are lists of the same size as the input lists, and contain the scaled values
 * of the input node properties scaled according to the specified scalers.
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

        // Create a Scaler supplier for each input property
        // Array properties are unrolled into multiple scalers
        var scalerSuppliers = IntStream.range(0, config.nodeProperties().size())
            .mapToObj(inputPos -> prepareScalers(config.nodeProperties().get(inputPos), pickScaler(config.scalers(), inputPos)))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        initializeArrays(scaledProperties, scalerSuppliers.size());

        // Materialize each scaler and apply it to all properties
        for (int outputPos = 0; outputPos < scalerSuppliers.size(); outputPos++) {
            var scaler = scalerSuppliers.get(outputPos).get();
            scaleProperty(scaledProperties, scaler, outputPos);
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
        var tasks = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            (partition) -> (Runnable) () -> partition.consume((nodeId) -> {
                var afterValue = scaler.scaleProperty(nodeId);
                double[] existingResult = scaledProperties.get(nodeId);
                existingResult[index] = afterValue;
            })
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);
    }

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

    private List<Supplier<Scaler>> prepareScalers(String propertyName, Scaler.Variant scalerVariant) {
        var nodeProperties = graph.nodeProperties(propertyName);

        if (nodeProperties == null) {
            throw new IllegalArgumentException(formatWithLocale(
                "Node property `%s` not found in graph with node properties: %s",
                propertyName,
                graph.availableNodeProperties()
            ));
        }

        switch (nodeProperties.valueType()) {
            case LONG:
            case DOUBLE:
                return List.of(() -> scalerVariant.create(
                    nodeProperties,
                    graph.nodeCount(),
                    config.concurrency(),
                    executor
                ));
            case FLOAT_ARRAY:
                int arrayLength = nodeProperties.doubleArrayValue(0).length;
                return IntStream.range(0, arrayLength)
                    .mapToObj(idx -> (Supplier<Scaler>)
                        () -> scalerVariant.create(
                            transformToDoubleProperty(propertyName, nodeProperties, arrayLength, idx),
                            graph.nodeCount(),
                            config.concurrency(),
                            executor
                        )
                    ).collect(Collectors.toList());
            case LONG_ARRAY:
            case DOUBLE_ARRAY:
            case UNKNOWN:
        }

        throw new UnsupportedOperationException(formatWithLocale(
            "Scaling node property `%s` of type `%s` is not supported",
            propertyName,
            nodeProperties.valueType().cypherName()
        ));
    }

    private DoubleNodeProperties transformToDoubleProperty(String propertyName, NodeProperties property, int expectedArrayLength, int idx) {
        return (nodeId) -> {
            var propertyValue = property.floatArrayValue(nodeId);

            if (propertyValue == null || propertyValue.length != expectedArrayLength) {
                throw new IllegalArgumentException(formatWithLocale(
                    "For scaling property `%s` expected array of length %d but got length %d for node %d",
                    propertyName,
                    expectedArrayLength,
                    Optional.ofNullable(propertyValue).map(v -> v.length).orElse(0),
                    nodeId
                ));
            }
            return propertyValue[idx];
        };
    }

    private Scaler.Variant pickScaler(List<Scaler.Variant> scalerVariants, int i) {
        if (scalerVariants.size() == 1) {
            // this supports syntactic sugar variant
            return scalerVariants.get(0);
        } else {
            return scalerVariants.get(i);
        }
    }
}
