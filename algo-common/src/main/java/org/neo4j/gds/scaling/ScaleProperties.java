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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * This algorithm takes as input a list of node property names and a scaler.
 * It applies the scaler to the node property values, respectively, and outputs a single node property.
 * The output node property values are lists of the same size as the input lists, and contain the scaled values
 * of the input node properties.
 */
public class ScaleProperties extends Algorithm<ScaleProperties.Result> {

    private final Graph graph;
    private final ScalePropertiesBaseConfig config;
    private final ExecutorService executor;

    public ScaleProperties(
        Graph graph,
        ScalePropertiesBaseConfig config,
        ProgressTracker progressTracker,
        ExecutorService executor
    ) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.executor = executor;
    }

    @Override
    public Result compute() {
        progressTracker.beginSubTask("ScaleProperties");
        var scaledProperties = HugeObjectArray.newArray(double[].class, graph.nodeCount());

        // Create a Scaler for each input property
        // Array properties are unrolled into multiple scalers
        progressTracker.beginSubTask("Prepare scalers");
        var scalers = config.nodeProperties().stream()
            .map(this::prepareScalers)
            .collect(Collectors.toList());
        progressTracker.endSubTask("Prepare scalers");

        var scalerStatistics = IntStream
            .range(0, scalers.size())
            .boxed()
            .collect(Collectors.toMap(config.nodeProperties()::get, i -> scalers.get(i).statistics()));

        var outputArrayLength = scalers.stream().mapToInt(Scaler::dimension).sum();
        initializeArrays(scaledProperties, outputArrayLength);

        // Apply scalers to all properties
        progressTracker.beginSubTask("Scale properties");
        var resultIndex = 0;
        for (var scaler : scalers) {
            scaleProperty(scaledProperties, scaler, resultIndex);
            resultIndex += scaler.dimension();
        }
        progressTracker.endSubTask("Scale properties");

        progressTracker.endSubTask("ScaleProperties");
        return Result.of(scaledProperties, scalerStatistics);
    }

    private void initializeArrays(HugeObjectArray<double[]> scaledProperties, int propertyCount) {
        var tasks = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            (partition) -> (Runnable) () -> partition.consume((nodeId) -> scaledProperties.set(
                nodeId,
                new double[propertyCount]
            )),
            Optional.empty()
        );
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .executor(executor)
            .run();
    }

    private void scaleProperty(HugeObjectArray<double[]> scaledProperties, Scaler scaler, int index) {
        var strategy = selectPropertyScalerStrategy(scaledProperties, scaler, index);
        var tasks = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            partition -> (Runnable) () -> {
                partition.consume(strategy);
                progressTracker.logProgress(partition.nodeCount());
            },
            Optional.empty()
        );
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .executor(executor)
            .run();
    }

    /**
     * If the property is a list property, we will use an optimised code path here.
     */
    private LongConsumer selectPropertyScalerStrategy(
        HugeObjectArray<double[]> scaledProperties,
        Scaler scaler,
        int index
    ) {
        if (scaler instanceof Scaler.ArrayScaler) {
            return nodeId -> ((Scaler.ArrayScaler) scaler).scaleProperty(nodeId, scaledProperties.get(nodeId), index);
        } else {
            return nodeId -> scaledProperties.get(nodeId)[index] = scaler.scaleProperty(nodeId);
        }
    }

    @ValueClass
    interface Result {
        HugeObjectArray<double[]> scaledProperties();

        Map<String, Map<String, List<Double>>> scalerStatistics();

        static Result of(HugeObjectArray<double[]> properties, Map<String, Map<String, List<Double>>> stats) {
            return ImmutableResult.of(properties, stats);
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

        int dimension = nodeProperties.dimension().orElseThrow(/* already validated in config */);
        List<ScalarScaler> elementScalers;

        switch (nodeProperties.valueType()) {
            case LONG:
            case DOUBLE:
                return scalerVariant.create(
                    nodeProperties,
                    graph.nodeCount(),
                    config.concurrency(),
                    progressTracker,
                    executor
                );
            case LONG_ARRAY:
                elementScalers = IntStream.range(0, dimension)
                    .mapToObj(idx -> scalerVariant.create(
                        transformLongArrayEntryToDoubleProperty(propertyName, nodeProperties, dimension, idx),
                        graph.nodeCount(),
                        config.concurrency(),
                        progressTracker,
                        executor
                    )).collect(Collectors.toList());
                return new Scaler.ArrayScaler(elementScalers, progressTracker);
            case FLOAT_ARRAY:
                elementScalers = IntStream.range(0, dimension)
                    .mapToObj(idx -> scalerVariant.create(
                        transformFloatArrayEntryToDoubleProperty(propertyName, nodeProperties, dimension, idx),
                        graph.nodeCount(),
                        config.concurrency(),
                        progressTracker,
                        executor
                    )).collect(Collectors.toList());
                return new Scaler.ArrayScaler(elementScalers, progressTracker);
            case DOUBLE_ARRAY:
                elementScalers = IntStream.range(0, dimension)
                    .mapToObj(idx -> scalerVariant.create(
                        transformDoubleArrayEntryToDoubleProperty(propertyName, nodeProperties, dimension, idx),
                        graph.nodeCount(),
                        config.concurrency(),
                        progressTracker,
                        executor
                    )).collect(Collectors.toList());
                return new Scaler.ArrayScaler(elementScalers, progressTracker);
            case UNKNOWN:
        }

        throw new UnsupportedOperationException(formatWithLocale(
            "Scaling node property `%s` of type `%s` is not supported",
            propertyName,
            nodeProperties.valueType().cypherName()
        ));
    }

    private DoubleNodePropertyValues transformFloatArrayEntryToDoubleProperty(
        String propertyName,
        NodePropertyValues property,
        int dimension,
        int idx
    ) {
        return new DoubleNodePropertyValues() {
            @Override
            public double doubleValue(long nodeId) {
                var propertyValue = property.floatArrayValue(nodeId);

                if (propertyValue == null || propertyValue.length != dimension) {
                    throw createInvalidArrayException(
                        propertyName,
                        dimension,
                        nodeId,
                        Optional.ofNullable(propertyValue).map(v -> v.length).orElse(0)
                    );
                }
                return propertyValue[idx];
            }

            @Override
            public long valuesStored() {
                return property.valuesStored();
            }

            @Override
            public long maxIndex() {
                return property.maxIndex();
            }
        };
    }

    private DoubleNodePropertyValues transformDoubleArrayEntryToDoubleProperty(
        String propertyName,
        NodePropertyValues property,
        int dimension,
        int idx
    ) {
        return new DoubleNodePropertyValues() {
            @Override
            public double doubleValue(long nodeId) {
                var propertyValue = property.doubleArrayValue(nodeId);

                if (propertyValue == null || propertyValue.length != dimension) {
                    throw createInvalidArrayException(
                        propertyName,
                        dimension,
                        nodeId,
                        Optional.ofNullable(propertyValue).map(v -> v.length).orElse(0)
                    );
                }
                return propertyValue[idx];
            }

            @Override
            public long valuesStored() {
                return property.valuesStored();
            }

            @Override
            public long maxIndex() {
                return property.maxIndex();
            }
        };
    }

    private DoubleNodePropertyValues transformLongArrayEntryToDoubleProperty(
        String propertyName,
        NodePropertyValues property,
        int dimension,
        int idx
    ) {
        return new DoubleNodePropertyValues() {
            @Override
            public double doubleValue(long nodeId) {
                var propertyValue = property.longArrayValue(nodeId);

                if (propertyValue == null || propertyValue.length != dimension) {
                    throw createInvalidArrayException(
                        propertyName,
                        dimension,
                        nodeId,
                        Optional.ofNullable(propertyValue).map(v -> v.length).orElse(0)
                    );
                }
                return propertyValue[idx];
            }

            @Override
            public long valuesStored() {
                return property.valuesStored();
            }

            @Override
            public long maxIndex() {
                return property.maxIndex();
            }
        };
    }

    private IllegalArgumentException createInvalidArrayException(
        String propertyName,
        int dimension,
        long nodeId,
        int actualLength
    ) {
        return new IllegalArgumentException(formatWithLocale(
            "For scaling property `%s` expected array of length %d but got length %d for node %d",
            propertyName,
            dimension,
            actualLength,
            graph.toOriginalNodeId(nodeId)
        ));
    }
}
