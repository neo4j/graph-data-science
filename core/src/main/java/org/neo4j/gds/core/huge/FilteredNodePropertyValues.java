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
package org.neo4j.gds.core.huge;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public abstract class FilteredNodePropertyValues implements NodePropertyValues {
    protected final NodePropertyValues properties;
    protected FilteredIdMap graph;

    protected abstract long translateId(long nodeId);

    FilteredNodePropertyValues(NodePropertyValues properties, FilteredIdMap graph) {
        this.properties = properties;
        this.graph = graph;
    }

    @Override
    public double doubleValue(long nodeId) {
        return properties.doubleValue(translateId(nodeId));
    }

    @Override
    public long longValue(long nodeId) {
        return properties.longValue(translateId(nodeId));
    }

    @Override
    public float[] floatArrayValue(long nodeId) {
        return properties.floatArrayValue(translateId(nodeId));
    }

    @Override
    public double[] doubleArrayValue(long nodeId) {
        return properties.doubleArrayValue(translateId(nodeId));
    }

    @Override
    public long[] longArrayValue(long nodeId) {
        return properties.longArrayValue(translateId(nodeId));
    }

    @Override
    public Object getObject(long nodeId) {
        return properties.getObject(translateId(nodeId));
    }

    @Override
    public ValueType valueType() {
        return properties.valueType();
    }

    @Override
    public Optional<Integer> dimension() {
        return properties.dimension();
    }

    @Override
    public OptionalLong getMaxLongPropertyValue() {
        if (valueType() == ValueType.LONG) {
            MutableLong currentMax = new MutableLong(Long.MIN_VALUE);
            graph.forEachNode(id -> {
                currentMax.setValue(Math.max(currentMax.doubleValue(), longValue(id)));
                return true;
            });
            return currentMax.longValue() == Long.MIN_VALUE
                ? OptionalLong.empty()
                : OptionalLong.of(currentMax.longValue());

        } else if (valueType() == ValueType.DOUBLE) {
            MutableDouble currentMax = new MutableDouble(Double.NEGATIVE_INFINITY);
            graph.forEachNode(id -> {
                currentMax.setValue(Math.max(currentMax.doubleValue(), doubleValue(id)));
                return true;
            });
            return currentMax.doubleValue() == Double.NEGATIVE_INFINITY
                ? OptionalLong.empty()
                : OptionalLong.of(currentMax.toDouble().longValue());

        } else {
            return OptionalLong.empty();
        }
    }

    @Override
    public OptionalDouble getMaxDoublePropertyValue() {
        if (valueType() == ValueType.LONG) {
            MutableLong currentMax = new MutableLong(Long.MIN_VALUE);
            graph.forEachNode(id -> {
                currentMax.setValue(Math.max(currentMax.doubleValue(), longValue(id)));
                return true;
            });
            return currentMax.longValue() == Long.MIN_VALUE
                ? OptionalDouble.empty()
                : OptionalDouble.of(currentMax.toLong().doubleValue());

        } else if (valueType() == ValueType.DOUBLE) {
            MutableDouble currentMax = new MutableDouble(Double.NEGATIVE_INFINITY);
            graph.forEachNode(id -> {
                currentMax.setValue(Math.max(currentMax.doubleValue(), doubleValue(id)));
                return true;
            });
            return currentMax.doubleValue() == Double.NEGATIVE_INFINITY
                ? OptionalDouble.empty()
                : OptionalDouble.of(currentMax.doubleValue());

        } else {
            return OptionalDouble.empty();
        }
    }

    @Override
    public long nodeCount() {
        return graph.nodeCount();
    }

    // This class is used when the ID space of the wrapped properties is wider than the id space used to retrieved node properties.
    public static class FilteredToOriginalNodePropertyValues extends FilteredNodePropertyValues {

        public FilteredToOriginalNodePropertyValues(NodePropertyValues properties, NodeFilteredGraph graph) {
            super(properties, graph);
        }

        @Override
        protected long translateId(long nodeId) {
            return graph.toRootNodeId(nodeId);
        }
    }

    // This class is used when the ID space of the wrapped properties is smaller than the id space used to retrieved node properties.
    public static final class OriginalToFilteredNodePropertyValues extends FilteredNodePropertyValues {

        private OriginalToFilteredNodePropertyValues(NodePropertyValues properties, FilteredIdMap graph) {
            super(properties, graph);
        }

        public static NodePropertyValues create(NodePropertyValues properties, FilteredIdMap graph) {
            return new OriginalToFilteredNodePropertyValues(properties, graph);
        }

        @Override
        public double doubleValue(long nodeId) {
            long translatedId = translateId(nodeId);

            if (translatedId < 0) {
                return DefaultValue.DOUBLE_DEFAULT_FALLBACK;
            }

            return properties.doubleValue(translatedId);
        }

        @Override
        public long longValue(long nodeId) {
            long translatedId = translateId(nodeId);

            if (translatedId < 0) {
                return DefaultValue.LONG_DEFAULT_FALLBACK;
            }
            return properties.longValue(translatedId);
        }

        @Override
        public float[] floatArrayValue(long nodeId) {
            long translatedId = translateId(nodeId);

            if (translatedId < 0) {
                return DefaultValue.DEFAULT.floatArrayValue();
            }

            return properties.floatArrayValue(translatedId);
        }

        @Override
        public double[] doubleArrayValue(long nodeId) {
            long translatedId = translateId(nodeId);

            if (translatedId < 0) {
                return DefaultValue.DEFAULT.doubleArrayValue();
            }

            return properties.doubleArrayValue(translatedId);
        }

        @Override
        public long[] longArrayValue(long nodeId) {
            long translatedId = translateId(nodeId);

            if (translatedId < 0) {
                return DefaultValue.DEFAULT.longArrayValue();
            }

            return properties.longArrayValue(translatedId);
        }

        @Override
        public Object getObject(long nodeId) {
            long translatedId = translateId(nodeId);

            if (translatedId < 0) {
                return null;
            }

            return properties.getObject(translatedId);
        }

        @Override
        protected long translateId(long nodeId) {
            return graph.toFilteredNodeId(nodeId);
        }
    }
}
