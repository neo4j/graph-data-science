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
package org.neo4j.gds.api;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.schema.Direction;

import java.util.Objects;

/**
 * Graph characteristics describe certain capabilities of the graph.
 * <p>
 * Algorithms define the graph characteristics that they require to
 * for correct execution. The execution framework can use both, the
 * graph and the required characteristics to check if the algorithm
 * can be run on the given graph.
 */
public final class GraphCharacteristics {

    public static final GraphCharacteristics ALL;
    public static final GraphCharacteristics NONE;

    static {
        ALL = GraphCharacteristics.builder().directed().undirected().inverseIndexed().build();
        NONE = GraphCharacteristics.builder().build();
    }

    private static final int DIRECTED = 1;
    private static final int UNDIRECTED = 1 << 1;
    private static final int INVERSE_INDEXED = 1 << 2;

    // We use a single int value to store the characteristics.
    // Each bit represents a characteristic. Setting / Getting
    // a characteristic is performed by bit-wise Or / And.
    private final int characteristics;

    public static GraphCharacteristics.Builder builder() {
        return new Builder();
    }

    private GraphCharacteristics(int characteristics) {
        this.characteristics = characteristics;
    }

    public boolean isDirected() {
        return (characteristics() & DIRECTED) == DIRECTED;
    }

    public boolean isUndirected() {
        return (characteristics() & UNDIRECTED) == UNDIRECTED;
    }

    public boolean isInverseIndexed() {
        return (characteristics() & INVERSE_INDEXED) == INVERSE_INDEXED;
    }

    private int characteristics() {
        return characteristics;
    }

    @Override
    public String toString() {
        return "GraphCharacteristics{" +
               "isDirected=" + isDirected() +
               ", isUndirected=" + isUndirected() +
               ", isInverseIndexed=" + isInverseIndexed() +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphCharacteristics that = (GraphCharacteristics) o;
        return characteristics == that.characteristics;
    }

    @Override
    public int hashCode() {
        return Objects.hash(characteristics);
    }

    public static class Builder {
        private int characteristics = 0;

        public Builder directed() {
            this.characteristics |= GraphCharacteristics.DIRECTED;
            return this;
        }

        public Builder undirected() {
            this.characteristics |= GraphCharacteristics.UNDIRECTED;
            return this;
        }

        public Builder inverseIndexed() {
            this.characteristics |= GraphCharacteristics.INVERSE_INDEXED;
            return this;
        }

        public Builder withOrientation(Orientation orientation) {
            switch (orientation) {
                case NATURAL:
                case REVERSE:
                    return this.directed();
                case UNDIRECTED:
                    return this.undirected();
                default:
                    throw new UnsupportedOperationException("Unexpected orientation: " + orientation);
            }
        }

        public Builder withDirection(Direction direction) {
            switch (direction) {
                case DIRECTED:
                    return this.directed();
                case UNDIRECTED:
                    return this.undirected();
                default:
                    throw new UnsupportedOperationException("Unexpected direction: " + direction);
            }
        }

        public GraphCharacteristics build() {
            return new GraphCharacteristics(this.characteristics);
        }
    }
}
