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
package org.neo4j.gds.beta.pregel;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.utils.StringFormatting;
import org.neo4j.gds.utils.StringJoining;

import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.api.nodeproperties.ValueType.DOUBLE;
import static org.neo4j.gds.api.nodeproperties.ValueType.DOUBLE_ARRAY;
import static org.neo4j.gds.api.nodeproperties.ValueType.LONG;
import static org.neo4j.gds.api.nodeproperties.ValueType.LONG_ARRAY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class NodeValue {

    private final PregelSchema pregelSchema;
    private final Map<String, ValueType> propertyTypes;

    NodeValue(PregelSchema pregelSchema) {
        this.pregelSchema = pregelSchema;
        this.propertyTypes = pregelSchema.elements()
            .stream()
            .collect(Collectors.toMap(Element::propertyKey, Element::propertyType));
    }

    static NodeValue of(PregelSchema schema, long nodeCount, int concurrency) {
        var properties = schema.elements()
            .stream()
            .collect(Collectors.toMap(
                Element::propertyKey,
                element -> initArray(element, nodeCount, concurrency)
            ));

        if (properties.size() == 1) {
            var element = schema.elements().iterator().next();
            var property = properties.values().iterator().next();
            return new SingleNodeValue(schema, element, property);
        }

        return new CompositeNodeValue(schema, properties);
    }

    static MemoryEstimation memoryEstimation(PregelSchema pregelSchema) {
        return MemoryEstimations.setup("", (dimensions, concurrency) -> {
            var builder = MemoryEstimations.builder();

            pregelSchema.elements().forEach(element -> {
                var entry = formatWithLocale("%s (%s)", element.propertyKey(), element.propertyType());

                switch (element.propertyType()) {
                    case LONG:
                        builder.fixed(entry, HugeLongArray.memoryEstimation(dimensions.nodeCount()));
                        break;
                    case DOUBLE:
                        builder.fixed(entry, HugeDoubleArray.memoryEstimation(dimensions.nodeCount()));
                        break;
                    case LONG_ARRAY:
                        builder.add(entry, MemoryEstimations.builder()
                            .fixed(
                                HugeObjectArray.class.getSimpleName(),
                                MemoryUsage.sizeOfInstance(HugeObjectArray.class)
                            )
                            .perNode("long[10]", nodeCount -> nodeCount * MemoryUsage.sizeOfLongArray(10))
                            .build());
                        break;
                    case DOUBLE_ARRAY:
                        builder.add(entry, MemoryEstimations.builder()
                            .fixed(
                                HugeObjectArray.class.getSimpleName(),
                                MemoryUsage.sizeOfInstance(HugeObjectArray.class)
                            )
                            .perNode("double[10]", nodeCount -> nodeCount * MemoryUsage.sizeOfDoubleArray(10))
                            .build());
                        break;
                    default:
                        builder.add(entry, MemoryEstimations.empty());
                }
            });

            return builder.build();
        });
    }

    public PregelSchema schema() {
        return pregelSchema;
    }

    public abstract HugeDoubleArray doubleProperties(String propertyKey);

    public abstract HugeLongArray longProperties(String propertyKey);

    public abstract HugeObjectArray<long[]> longArrayProperties(String propertyKey);

    public abstract HugeObjectArray<double[]> doubleArrayProperties(String propertyKey);

    public double doubleValue(String key, long nodeId) {
        return doubleProperties(key).get(nodeId);
    }

    public long longValue(String key, long nodeId) {
        return longProperties(key).get(nodeId);
    }

    public long[] longArrayValue(String key, long nodeId) {
        HugeObjectArray<long[]> arrayProperties = longArrayProperties(key);
        return arrayProperties.get(nodeId);
    }

    public double[] doubleArrayValue(String key, long nodeId) {
        HugeObjectArray<double[]> arrayProperties = doubleArrayProperties(key);
        return arrayProperties.get(nodeId);
    }

    public void set(String key, long nodeId, double value) {
        doubleProperties(key).set(nodeId, value);
    }

    public void set(String key, long nodeId, long value) {
        longProperties(key).set(nodeId, value);
    }

    public void set(String key, long nodeId, long[] value) {
        longArrayProperties(key).set(nodeId, value);
    }

    public void set(String key, long nodeId, double[] value) {
        doubleArrayProperties(key).set(nodeId, value);
    }

    void checkProperty(String key, ValueType expectedType) {
        checkProperty(key, propertyTypes.get(key), expectedType);
    }

    private void checkProperty(String key, @Nullable ValueType actualType, ValueType expectedType) {
        if (actualType == null) {
            throw new IllegalArgumentException(formatWithLocale(
                "Property with key %s does not exist. Available properties are: %s",
                key,
                propertyTypes.keySet()
            ));
        }

        if (actualType != expectedType) {
            throw new IllegalArgumentException(formatWithLocale(
                "Requested property type %s is not compatible with available property type %s for key %s. " +
                "Available property types: %s",
                expectedType,
                actualType,
                key,
                StringJoining.join(propertyTypes.entrySet().stream().map(e -> e.getKey() + ": " + e.getValue()))
            ));
        }
    }

    private static Object initArray(Element element, long nodeCount, int concurrency) {
        switch (element.propertyType()) {
            case DOUBLE:
                var doubleNodeValues = HugeDoubleArray.newArray(nodeCount);
                ParallelUtil.parallelForEachNode(
                    nodeCount,
                    concurrency,
                    TerminationFlag.RUNNING_TRUE,
                    nodeId -> doubleNodeValues.set(nodeId, DefaultValue.DOUBLE_DEFAULT_FALLBACK)
                );
                return doubleNodeValues;
            case LONG:
                var longNodeValues = HugeLongArray.newArray(nodeCount);
                ParallelUtil.parallelForEachNode(
                    nodeCount,
                    concurrency,
                    TerminationFlag.RUNNING_TRUE,
                    nodeId -> longNodeValues.set(nodeId, DefaultValue.LONG_DEFAULT_FALLBACK)
                );
                return longNodeValues;
            case LONG_ARRAY:
                return HugeObjectArray.newArray(long[].class, nodeCount);
            case DOUBLE_ARRAY:
                return HugeObjectArray.newArray(double[].class, nodeCount);
            default:
                throw new IllegalArgumentException(StringFormatting.formatWithLocale(
                    "Unsupported value type: %s",
                    element.propertyType()
                ));
        }
    }

    public static final class SingleNodeValue extends NodeValue {

        private final Element element;
        private final Object property;

        SingleNodeValue(PregelSchema pregelSchema, Element element, Object property) {
            super(pregelSchema);
            this.element = element;
            this.property = property;
        }

        @Override
        public HugeDoubleArray doubleProperties(String propertyKey) {
            checkProperty(propertyKey, DOUBLE);
            return (HugeDoubleArray) property;
        }

        @Override
        public HugeLongArray longProperties(String propertyKey) {
            checkProperty(propertyKey, LONG);
            return (HugeLongArray) property;
        }

        @Override
        public HugeObjectArray<long[]> longArrayProperties(String propertyKey) {
            checkProperty(propertyKey, LONG_ARRAY);
            //noinspection unchecked
            return (HugeObjectArray<long[]>) property;
        }

        @Override
        public HugeObjectArray<double[]> doubleArrayProperties(String propertyKey) {
            checkProperty(propertyKey, DOUBLE_ARRAY);
            //noinspection unchecked
            return (HugeObjectArray<double[]>) property;
        }

        @Override
        void checkProperty(String key, ValueType expectedType) {
            var actualType = element.propertyKey().equals(key) ? element.propertyType() : null;
            super.checkProperty(key, actualType, expectedType);
        }
    }

    public static final class CompositeNodeValue extends NodeValue {

        private final Map<String, Object> properties;

        CompositeNodeValue(PregelSchema pregelSchema, Map<String, Object> properties) {
            super(pregelSchema);
            this.properties = properties;
        }

        @Override
        public HugeDoubleArray doubleProperties(String propertyKey) {
            checkProperty(propertyKey, DOUBLE);
            return (HugeDoubleArray) properties.get(propertyKey);
        }

        @Override
        public HugeLongArray longProperties(String propertyKey) {
            checkProperty(propertyKey, LONG);
            return (HugeLongArray) properties.get(propertyKey);
        }

        @Override
        public HugeObjectArray<long[]> longArrayProperties(String propertyKey) {
            checkProperty(propertyKey, LONG_ARRAY);
            //noinspection unchecked
            return (HugeObjectArray<long[]>) properties.get(propertyKey);
        }

        @Override
        public HugeObjectArray<double[]> doubleArrayProperties(String propertyKey) {
            checkProperty(propertyKey, DOUBLE_ARRAY);
            //noinspection unchecked
            return (HugeObjectArray<double[]>) properties.get(propertyKey);
        }
    }
}
