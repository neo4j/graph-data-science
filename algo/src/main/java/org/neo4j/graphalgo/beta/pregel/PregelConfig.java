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
package org.neo4j.graphalgo.beta.pregel;

public final class PregelConfig {
    private final double initialNodeValue;
    private final boolean isAsynchronous;

    private PregelConfig(double initialNodeValue, boolean isAsynchronous) {
        this.initialNodeValue = initialNodeValue;
        this.isAsynchronous = isAsynchronous;
    }

    double getInitialNodeValue() {
        return initialNodeValue;
    }

    boolean isAsynchronous() {
        return isAsynchronous;
    }

    public static class Builder {
        private double initialNodeValue = -1.0;
        private boolean isAsynchronous = false;

        public Builder withInitialNodeValue(double initialNodeValue) {
            this.initialNodeValue = initialNodeValue;
            return this;
        }

        public Builder isAsynchronous(boolean isAsynchronous) {
            this.isAsynchronous = isAsynchronous;
            return this;
        }

        public PregelConfig build() {
            return new PregelConfig(initialNodeValue, isAsynchronous);
        }
    }
}
