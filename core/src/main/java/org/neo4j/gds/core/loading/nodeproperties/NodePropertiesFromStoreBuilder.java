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
import org.neo4j.gds.collections.hsa.HugeSparseCollections;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.HighLimitIdMap;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.values.GdsNoValue;
import org.neo4j.gds.values.GdsValue;
import org.neo4j.gds.values.primitive.PrimitiveValues;

import java.util.concurrent.atomic.AtomicReference;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

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
        Concurrency concurrency
    ) {
        return new NodePropertiesFromStoreBuilder(defaultValue, concurrency);
    }

    private final DefaultValue defaultValue;
    private final Concurrency concurrency;
    private final AtomicReference<InnerNodePropertiesBuilder> innerBuilder;

    private NodePropertiesFromStoreBuilder(
        DefaultValue defaultValue,
        Concurrency concurrency
    ) {
        this.defaultValue = defaultValue;
        this.concurrency = concurrency;
        this.innerBuilder = new AtomicReference<>();
    }

    public void set(long neoNodeId, GdsValue value) {
        if (value != null && value != GdsNoValue.NO_VALUE) {
            if (innerBuilder.get() == null) {
                initializeWithType(value);
            }
            innerBuilder.get().setValue(neoNodeId, value);
        }
    }

    public NodePropertyValues build(IdMap idMap) {
        if (innerBuilder.get() == null) {
            if (defaultValue.getObject() != null) {
                var gdsValue = PrimitiveValues.create(defaultValue.getObject());
                initializeWithType(gdsValue);
            } else {
                throw new IllegalStateException("Cannot infer type of property");
            }
        }

        // For HighLimitIdMap, we need to use the rootIdMap to resolve intermediate
        // node ids correctly. The rootIdMap in that case is the mapping between
        // intermediate and mapped node ids. The imported property values are associated
        // with the intermediate node ids.
        var actualIdMap = (idMap instanceof HighLimitIdMap) ? idMap.rootIdMap() : idMap;

        return innerBuilder.get().build(idMap.nodeCount(), actualIdMap, idMap.highestOriginalId());
    }

    // This is synchronized as we want to prevent the creation of multiple InnerNodePropertiesBuilders of which only once survives.
    private synchronized void initializeWithType(GdsValue value) {
        if (innerBuilder.get() == null) {
            var newBuilder = newInnerBuilder(value.type());
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
