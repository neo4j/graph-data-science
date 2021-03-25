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
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

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

        var resolvedProperties = config
            .nodeProperties()
            .stream()
            .map(this::resolveNodeProperty)
            .collect(Collectors.toList());

        int outputLength = resolvedProperties.stream()
            .mapToInt(NodePropertyLength::propertyLength)
            .sum();
        initializeArrays(scaledProperties, outputLength);

        var scalers = resolveScalers(resolvedProperties);
        for (int idx = 0; idx < scalers.size(); idx++) {
            scaleProperty(scaledProperties, scalers.get(idx), idx);
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

    private List<Scaler> resolveScalers(List<NodePropertyLength> resolvedProperties) {
        var scalers = new ArrayList<Scaler>();

        for (int i = 0; i < resolvedProperties.size(); i++) {
            var scalerVariant = pickScaler(config.scalers(), i);
            var property = resolvedProperties.get(i);

            if (property.properties().valueType() == ValueType.FLOAT_ARRAY) {
                for (int arrayIdx = 0; arrayIdx < property.propertyLength(); arrayIdx++) {
                    scalers.add(scalerVariant.create(
                        transformToDoubleProperty(i, property, arrayIdx),
                        graph.nodeCount(),
                        config.concurrency(),
                        executor
                    ));
                }
            } else {
                scalers.add(scalerVariant.create(
                    property.properties(),
                    graph.nodeCount(),
                    config.concurrency(),
                    executor
                ));
            }
        }
        return scalers;
    }

    private DoubleNodeProperties transformToDoubleProperty(int propertyIdx, NodePropertyLength property, int idx) {
        return (nodeId) -> {
            var array = property
                .properties()
                .floatArrayValue(nodeId);

            if (array == null || array.length != property.propertyLength()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "For scaling property `%s` expected array of length %d but got length %d for node %d",
                    config.nodeProperties().get(propertyIdx),
                    property.propertyLength(),
                    Optional.ofNullable(array).map(v -> v.length).orElse(0),
                    nodeId
                ));
            }
            return array[idx];
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

    // TODO move nodeProperty check to org.neo4j.graphalgo.api.GraphStoreValidation when moving to beta
    private NodePropertyLength resolveNodeProperty(String property) {
        NodeProperties result = graph.nodeProperties(property);
        // TODO support Long and Double arrays
        var supportedTypes = Set.of(ValueType.DOUBLE, ValueType.LONG, ValueType.FLOAT_ARRAY);

        if (result == null) {
            throw new IllegalArgumentException(formatWithLocale(
                "Node property `%s` not found in graph with node properties: %s",
                property,
                graph.availableNodeProperties()
            ));
        } else if (!supportedTypes.contains(result.valueType())) {
            throw new UnsupportedOperationException(formatWithLocale(
                "Scaling node property `%s` of type `%s` is not supported. Supported types are %s",
                property,
                result.valueType().cypherName(),
                supportedTypes.stream().map(ValueType::cypherName).collect(Collectors.joining(", "))
            ));
        }

        switch (result.valueType()) {
            case LONG:
            case DOUBLE:
                return ImmutableNodePropertyLength.of(result, 1);
            case FLOAT_ARRAY:
                return ImmutableNodePropertyLength.of(result, result.doubleArrayValue(0).length);
            case LONG_ARRAY:
            case DOUBLE_ARRAY:
            case UNKNOWN:

        }

        return null;
    }

    @ValueClass
    interface NodePropertyLength {
        NodeProperties properties();

        int propertyLength();
    }

}
