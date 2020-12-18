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
package org.neo4j.graphalgo.api.nodeproperties;

import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.values.storable.NumberType;

public enum ValueType {
    LONG {
        @Override
        public String cypherName() {
            return "Integer";
        }

        @Override
        public String csvName() {
            return "long";
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forLong();
        }
    },
    DOUBLE {
        @Override
        public String cypherName() {
            return "Float";
        }

        @Override
        public String csvName() {
            return "double";
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forDouble();
        }
    },
    DOUBLE_ARRAY {
        @Override
        public String cypherName() {
            return "List of Float";
        }

        @Override
        public String csvName() {
            return "double[]";
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forDoubleArray();
        }
    },
    FLOAT_ARRAY {
        @Override
        public String cypherName() {
            return "List of Float";
        }

        @Override
        public String csvName() {
            return "float[]";
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forFloatArray();
        }
    },
    LONG_ARRAY {
        @Override
        public String cypherName() {
            return "List of Integer";
        }

        @Override
        public String csvName() {
            return "long[]";
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forLongArray();
        }
    },
    UNKNOWN {
        @Override
        public String cypherName() {
            return "Unknown";
        }

        @Override
        public String csvName() {
            throw new UnsupportedOperationException("Value Type UKNONWN is not supported in CSV");
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.DEFAULT;
        }
    };

    public abstract String cypherName();

    public abstract String csvName();

    public abstract DefaultValue fallbackValue();

    public static ValueType fromNumberType(NumberType nt) {
        switch (nt) {
            case FLOATING_POINT:
                return DOUBLE;
            case INTEGRAL:
                return LONG;
            case NO_NUMBER:
                return UNKNOWN;
            default:
                throw new IllegalArgumentException("Unexpected value: " + nt + " (sad java 😞)");
        }
    }
}
