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
package org.neo4j.graphalgo.beta.pregel;

public interface Reducer {

    /**
     * The identity element is used as the initial value.
     */
    double identity();

    /**
     * Computes a new value based on the current value and the message.
     */
    double reduce(double current, double message);

    /**
     * The empty value is used when a node did not receive any messages.
     */
    double emptyValue();

    /**
     * Checks if the given value is the empty value.
     */
    boolean isEmptyValue(double value);

    class Sum implements Reducer {

        @Override
        public double identity() {
            return 0;
        }

        @Override
        public double reduce(double current, double message) {
            return current + message;
        }

        @Override
        public double emptyValue() {
            return Double.NaN;
        }

        @Override
        public boolean isEmptyValue(double value) {
            return Double.isNaN(value);
        }
    }

    class Min implements Reducer {

        @Override
        public double identity() {
            return Double.MAX_VALUE;
        }

        @Override
        public double reduce(double current, double message) {
            return Math.min(current, message);
        }

        @Override
        public double emptyValue() {
            return Double.NaN;
        }

        @Override
        public boolean isEmptyValue(double value) {
            return Double.isNaN(value);
        }
    }

    class Max implements Reducer {

        @Override
        public double identity() {
            return -Double.MAX_VALUE;
        }

        @Override
        public double reduce(double current, double message) {
            return Math.max(current, message);
        }

        @Override
        public double emptyValue() {
            return Double.NaN;
        }

        @Override
        public boolean isEmptyValue(double value) {
            return Double.isNaN(value);
        }
    }

    class Count implements Reducer {

        @Override
        public double identity() {
            return 0;
        }

        @Override
        public double reduce(double current, double message) {
            return current + 1;
        }

        @Override
        public double emptyValue() {
            return Double.NaN;
        }

        @Override
        public boolean isEmptyValue(double value) {
            return Double.isNaN(value);
        }
    }
}
