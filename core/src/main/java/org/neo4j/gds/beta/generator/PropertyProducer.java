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
package org.neo4j.gds.beta.generator;

import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.Objects;
import java.util.Random;

public interface PropertyProducer<PROPERTY_SLICE> {

    static PropertyProducer<double[]> fixedDouble(String propertyName, double value) {
        return new FixedDoubleProducer(propertyName, value);
    }

    static PropertyProducer<double[]> randomDouble(String propertyName, double min, double max) {
        return new RandomDoubleProducer(propertyName, min, max);
    }

    static PropertyProducer<float[][]> randomEmbedding(String propertyName, int embeddingSize, float min, float max) {
        return new RandomEmbeddingProducer(propertyName, embeddingSize, min, max);
    }

    static PropertyProducer<long[]> nodeIdAsLong(String propertyName) {
        return new NodeIdProducer(propertyName);
    }

    static PropertyProducer<long[]> fixedLong(String propertyName, long value) {
        return new FixedLongProducer(propertyName, value);
    }

    static PropertyProducer<long[]> randomLong(String propertyName, long min, long max) {
        return new RandomLongProducer(propertyName, min, max);
    }

    static PropertyProducer<long[][]> randomLongArray(String propertyName, int length, long min, long max) {
        return new RandomLongArrayProducer(propertyName, length, min, max);
    }

    static PropertyProducer<double[][]> randomDoubleArray(String propertyName, int length, double min, double max) {
        return new RandomDoubleArrayProducer(propertyName, length, min, max);
    }

    String getPropertyName();

    ValueType propertyType();

    void setProperty(long nodeId, PROPERTY_SLICE slice, int index, Random random);

    class FixedDoubleProducer implements PropertyProducer<double[]> {
        private final String propertyName;
        private final double value;

        public FixedDoubleProducer(String propertyName, double value) {
            this.propertyName = propertyName;
            this.value = value;
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public ValueType propertyType() {
            return ValueType.DOUBLE;
        }

        @Override
        public void setProperty(long nodeId, double[] doubles, int index, Random random) {
            doubles[index] = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FixedDoubleProducer fixedDoubleProducer = (FixedDoubleProducer) o;
            return Double.compare(fixedDoubleProducer.value, value) == 0 &&
                   Objects.equals(propertyName, fixedDoubleProducer.propertyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyName, value);
        }

        @Override
        public String toString() {
            return "FixedDoubleProducer{" +
                   "propertyName='" + propertyName + '\'' +
                   ", value=" + value +
                   '}';
        }
    }

    class FixedLongProducer implements PropertyProducer<long[]> {
        private final String propertyName;
        private final long value;

        public FixedLongProducer(String propertyName, long value) {
            this.propertyName = propertyName;
            this.value = value;
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public ValueType propertyType() {
            return ValueType.LONG;
        }

        @Override
        public void setProperty(long nodeId, long[] longs, int index, Random random) {
            longs[index] = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FixedLongProducer fixedLongProducer = (FixedLongProducer) o;
            return fixedLongProducer.value == value &&
                Objects.equals(propertyName, fixedLongProducer.propertyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyName, value);
        }

        @Override
        public String toString() {
            return "FixedLongProducer{" +
                "propertyName='" + propertyName + '\'' +
                ", value=" + value +
                '}';
        }
    }

    class RandomDoubleProducer implements PropertyProducer<double[]> {
        private final String propertyName;
        private final double min;
        private final double max;

        public RandomDoubleProducer(String propertyName, double min, double max) {
            this.propertyName = propertyName;
            this.min = min;
            this.max = max;

            if (max <= min) {
                throw new IllegalArgumentException("Max value must be greater than min value");
            }
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public ValueType propertyType() {
            return ValueType.DOUBLE;
        }

        @Override
        public void setProperty(long nodeId, double[] doubles, int index, Random random) {
            doubles[index] = min + (random.nextDouble() * (max - min));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RandomDoubleProducer random = (RandomDoubleProducer) o;
            return Double.compare(random.min, min) == 0 &&
                   Double.compare(random.max, max) == 0 &&
                   Objects.equals(propertyName, random.propertyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyName, min, max);
        }

        @Override
        public String toString() {
            return "RandomDoubleProducer{" +
                   "propertyName='" + propertyName + '\'' +
                   ", min=" + min +
                   ", max=" + max +
                   '}';
        }
    }

    class RandomEmbeddingProducer implements PropertyProducer<float[][]> {
        private final String propertyName;
        private final int embeddingSize;
        private final float min;
        private final float max;

        public RandomEmbeddingProducer(String propertyName, int embeddingSize, float min, float max) {
            this.propertyName = propertyName;
            this.embeddingSize = embeddingSize;
            this.min = min;
            this.max = max;

            if (max <= min) {
                throw new IllegalArgumentException("Max value must be greater than min value");
            }
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public ValueType propertyType() {
            return ValueType.FLOAT_ARRAY;
        }

        @Override
        public void setProperty(long nodeId, float[][] embeddings, int index, Random random) {
            var nodeEmbeddings = new float[embeddingSize];
            for (int i = 0; i < embeddingSize; i++) {
                nodeEmbeddings[i] = min + (random.nextFloat() * (max - min));
            }
            embeddings[index] = nodeEmbeddings;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RandomEmbeddingProducer random = (RandomEmbeddingProducer) o;
            return random.embeddingSize == embeddingSize &&
                   Double.compare(random.min, min) == 0 &&
                   Double.compare(random.max, max) == 0 &&
                   Objects.equals(propertyName, random.propertyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyName, embeddingSize, min, max);
        }

        @Override
        public String toString() {
            return "RandomDoubleProducer{" +
                   "propertyName='" + propertyName + '\'' +
                   ", embeddingSize=" + embeddingSize +
                   ", min=" + min +
                   ", max=" + max +
                   '}';
        }
    }

    class NodeIdProducer implements PropertyProducer<long[]> {
        private final String propertyName;

        NodeIdProducer(String propertyName) {
            this.propertyName = propertyName;
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public ValueType propertyType() {
            return ValueType.LONG;
        }

        @Override
        public void setProperty(long nodeId, long[] longs, int index, Random random) {
            longs[index] = nodeId;
        }
    }

    class RandomLongProducer implements PropertyProducer<long[]> {

        private final String propertyName;
        private final long min;
        private final long max;

        RandomLongProducer(String propertyName, long min, long max) {
            this.propertyName = propertyName;
            this.min = min;
            this.max = max;

            if (max <= min) {
                throw new IllegalArgumentException("Max value must be greater than min value");
            }
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public ValueType propertyType() {
            return ValueType.LONG;
        }

        @Override
        public void setProperty(long nodeId, long[] longs, int index, Random random) {
            var modulo = random.nextLong() % (max - min);
            if (modulo >= 0) {
                longs[index] = modulo + min;
            } else {
                longs[index] = modulo + max;
            }
        }
    }

    class RandomLongArrayProducer implements PropertyProducer<long[][]> {

        private final String propertyName;
        private final int length;
        private final long min;
        private final long max;

        RandomLongArrayProducer(String propertyName, int length, long min, long max) {
            this.propertyName = propertyName;
            this.length = length;
            this.min = min;
            this.max = max;

            if (max <= min) {
                throw new IllegalArgumentException("Max value must be greater than min value");
            }
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public ValueType propertyType() {
                return ValueType.LONG_ARRAY;
        }

        @Override
        public void setProperty(long nodeId, long[][] longs, int index, Random random) {
            var value = new long[length];
            for (int i = 0; i < length; i++) {
                var modulo = random.nextLong() % (max - min);
                if (modulo >= 0) {
                    value[i] = modulo + min;
                } else {
                    value[i] = modulo + max;
                }
            }
            longs[index] = value;
        }
    }

    class RandomDoubleArrayProducer implements PropertyProducer<double[][]> {
        private final String propertyName;
        private final int length;
        private final double min;
        private final double max;

        RandomDoubleArrayProducer(String propertyName, int length, double min, double max) {
            this.propertyName = propertyName;
            this.length = length;
            this.min = min;
            this.max = max;

            if (max <= min) {
                throw new IllegalArgumentException("Max value must be greater than min value");
            }
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public ValueType propertyType() {
            return ValueType.DOUBLE_ARRAY;
        }

        @Override
        public void setProperty(long nodeId, double[][] embeddings, int index, Random random) {
            var value = new double[length];
            for (int i = 0; i < length; i++) {
                value[i] = min + (random.nextDouble() * (max - min));
            }
            embeddings[index] = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RandomDoubleArrayProducer random = (RandomDoubleArrayProducer) o;
            return random.length == length &&
                   Double.compare(random.min, min) == 0 &&
                   Double.compare(random.max, max) == 0 &&
                   Objects.equals(propertyName, random.propertyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyName, length, min, max);
        }

        @Override
        public String toString() {
            return "RandomDoubleProducer{" +
                   "propertyName='" + propertyName + '\'' +
                   ", length=" + length +
                   ", min=" + min +
                   ", max=" + max +
                   '}';
        }
    }

    class EmptyPropertyProducer implements PropertyProducer<double[]> {
        @Override
        public String getPropertyName() {
            return null;
        }

        @Override
        public ValueType propertyType() {
            return ValueType.DOUBLE;
        }

        @Override
        public void setProperty(long nodeId, double[] doubles, int index, Random random) {
        }
    }
}
