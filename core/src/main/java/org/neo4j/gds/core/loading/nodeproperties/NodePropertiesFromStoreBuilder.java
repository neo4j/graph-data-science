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
package org.neo4j.gds.core.loading.nodeproperties;

import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.HugeSparseCollections;
import org.neo4j.gds.core.loading.ValueConverter;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.concurrent.atomic.AtomicReference;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.values.storable.Values.NO_VALUE;

public final class NodePropertiesFromStoreBuilder {

    private static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations
        .builder(NodePropertiesFromStoreBuilder.class)
        .rangePerGraphDimension(
            "property values",
            (dimensions, concurrency) -> HugeSparseCollections.estimateLong(
                dimensions.nodeCount(),
                dimensions.nodeCount()
            )
        )
        .build();

    public static MemoryEstimation memoryEstimation() {
        return MEMORY_ESTIMATION;
    }

    public static NodePropertiesFromStoreBuilder of(
        DefaultValue defaultValue,
        int concurrency
    ) {
        return new NodePropertiesFromStoreBuilder(defaultValue, concurrency);
    }

    private final DefaultValue defaultValue;
    private final int concurrency;
    private final AtomicReference<InnerNodePropertiesBuilder> innerBuilder;

    private NodePropertiesFromStoreBuilder(
        DefaultValue defaultValue,
        int concurrency
    ) {
        this.defaultValue = defaultValue;
        this.concurrency = concurrency;
        this.innerBuilder = new AtomicReference<>();
    }

    public void set(long neoNodeId, Value value) {
        if (value != null && value != NO_VALUE) {
            if (innerBuilder.get() == null) {
                initializeWithType(value);
            }
            innerBuilder.get().setValue(neoNodeId, value);
        }
    }

    public NodePropertyValues build(IdMap idMap) {
        if (innerBuilder.get() == null) {
            if (defaultValue.getObject() != null) {
                initializeWithType(Values.of(defaultValue.getObject()));
            } else {
                throw new IllegalStateException("Cannot infer type of property");
            }
        }

        return innerBuilder.get().build(idMap.nodeCount(), idMap, idMap.highestOriginalId());
    }

    // This is synchronized as we want to prevent the creation of multiple InnerNodePropertiesBuilders of which only once survives.
    private synchronized void initializeWithType(Value value) {
        if (innerBuilder.get() == null) {
            var valueType = ValueConverter.valueType(value);
            var newBuilder = newInnerBuilder(valueType);
            innerBuilder.compareAndSet(null, newBuilder);
        }
    }

    private InnerNodePropertiesBuilder newInnerBuilder(ValueType valueType) {
        switch (valueType) {
            case LONG:
                return LongNodePropertiesBuilder.of(defaultValue, concurrency);
            case DOUBLE:
                return new DoubleNodePropertiesBuilder(defaultValue, concurrency);
            case DOUBLE_ARRAY:
                return new DoubleArrayNodePropertiesBuilder(defaultValue, concurrency);
            case FLOAT_ARRAY:
                return new FloatArrayNodePropertiesBuilder(defaultValue, concurrency);
            case LONG_ARRAY:
                return new LongArrayNodePropertiesBuilder(defaultValue, concurrency);
            default:
                throw new UnsupportedOperationException(formatWithLocale(
                    "Loading of values of type %s is currently not supported",
                    valueType
                ));
        }
    }
}
