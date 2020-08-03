/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading.nodeproperties;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

public final class NodePropertiesFromStoreBuilder {

    private final DefaultValue defaultValue;
    private final long nodeSize;
    private final AllocationTracker tracker;
    private InnerNodePropertiesBuilder innerBuilder;
    private final LongAdder size;

    private final AtomicBoolean isInitialized;

    public static NodePropertiesFromStoreBuilder of(long nodeSize, AllocationTracker tracker, DefaultValue defaultValue) {
        return new NodePropertiesFromStoreBuilder(defaultValue, nodeSize, tracker);
    }

    @TestOnly
    public static NodeProperties of(long size, Object defaultValue, Consumer<NodePropertiesFromStoreBuilder> buildBlock) {
        var builder = of(size, AllocationTracker.EMPTY, DefaultValue.of(defaultValue));
        buildBlock.accept(builder);
        return builder.build();
    }

    private NodePropertiesFromStoreBuilder(DefaultValue defaultValue, long nodeSize, AllocationTracker tracker) {
        this.defaultValue = defaultValue;
        this.nodeSize = nodeSize;
        this.tracker = tracker;
        this.size = new LongAdder();
        this.isInitialized = new AtomicBoolean(false);
    }

    public void set(long nodeId, Value value) {
        if (value != null) {
            if (!isInitialized.get()) {
                initializeWithType(value);
            }
            if (isInitialized.get()) {
                innerBuilder.setValue(nodeId, value);
                size.increment();
            }
        }
    }

    public NodeProperties build() {
        if (innerBuilder == null) {
            if (defaultValue.getObject() != null) {
                initializeWithType(Values.of(defaultValue.getObject()));
            } else {
                throw new IllegalStateException("Cannot infer type of property");
            }
        }
        return innerBuilder.build(this.size.sum());
    }

    private synchronized void initializeWithType(Value value) {
        if (!isInitialized.get()) {
            if (value instanceof LongValue || value instanceof IntValue) {
                this.innerBuilder = new LongNodePropertiesBuilder(nodeSize, defaultValue, tracker);
            } else if (value instanceof DoubleValue || value instanceof FloatValue) {
                this.innerBuilder = new DoubleNodePropertiesBuilder(nodeSize, defaultValue, tracker);
            } else {
                throw new IllegalArgumentException("TODO"); //TODO
            }
            this.isInitialized.set(true);
        }
    }

}
