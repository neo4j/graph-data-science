/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.impl.generator;

import java.util.Objects;

public interface RelationshipPropertyProducer {

    static RelationshipPropertyProducer fixed(String propertyName, double value) {
        return new Fixed(propertyName, value);
    }

    static RelationshipPropertyProducer random(String propertyName, double min, double max, Long seed) {
        return new Random(propertyName, min, max, seed);
    }

    static RelationshipPropertyProducer random(String propertyName, double min, double max) {
        return new Random(propertyName, min, max, null);
    }

    String getPropertyName();

    double getPropertyValue(long source, long target);

    Long seed();

    class Fixed implements RelationshipPropertyProducer {
        private final String propertyName;
        private final double value;

        public Fixed(String propertyName, double value) {
            this.propertyName = propertyName;
            this.value = value;}

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public double getPropertyValue(long source, long target) {
            return value;
        }

        @Override
        public Long seed() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Fixed fixed = (Fixed) o;
            return Double.compare(fixed.value, value) == 0 &&
                   Objects.equals(propertyName, fixed.propertyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyName, value);
        }

        @Override
        public String toString() {
            return "Fixed{" +
                   "propertyName='" + propertyName + '\'' +
                   ", value=" + value +
                   '}';
        }
    }

    class Random implements RelationshipPropertyProducer {
        private final java.util.Random random;
        private final String propertyName;
        private final double min;
        private final double max;
        private final Long seed;

        public Random(String propertyName, double min, double max) {
            this(propertyName, min, max, null);
        }

        public Random(String propertyName, double min, double max, Long seed) {
            this.propertyName = propertyName;
            this.min = min;
            this.max = max;
            this.seed = seed;
            this.random = new java.util.Random();
            if(seed != null) {
                this.random.setSeed(seed);
            }

            if (max <= min) {
                throw new IllegalArgumentException("Max value must be greater than min value");
            }
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public double getPropertyValue(long source, long target) {
            return min + (random.nextDouble() * (max - min));
        }

        @Override
        public Long seed() {
            return this.seed;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Random random = (Random) o;
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
            return "Random{" +
                   "propertyName='" + propertyName + '\'' +
                   ", min=" + min +
                   ", max=" + max +
                   '}';
        }
    }
}
