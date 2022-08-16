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
package org.neo4j.gds.api.nodeproperties;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.DefaultValue;

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

        @Override
        public <RESULT> RESULT accept(Visitor<RESULT> visitor) {
            return visitor.visitLong();
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

        @Override
        public <RESULT> RESULT accept(Visitor<RESULT> visitor) {
            return visitor.visitDouble();
        }
    },
    STRING {
        @Override
        public String cypherName() {
            return "String";
        }

        @Override
        public String csvName() {
            return "string";
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.DEFAULT;
        }

        @Override
        public <RESULT> RESULT accept(Visitor<RESULT> visitor) {
            return visitor.visitString();
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

        @Override
        public <RESULT> RESULT accept(Visitor<RESULT> visitor) {
            return visitor.visitDoubleArray();
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

        @Override
        public <RESULT> RESULT accept(Visitor<RESULT> visitor) {
            return visitor.visitFloatArray();
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

        @Override
        public <RESULT> RESULT accept(Visitor<RESULT> visitor) {
            return visitor.visitLongArray();
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

        @Override
        public <RESULT> RESULT accept(Visitor<RESULT> visitor) {
            return visitor.visitUnknown();
        }
    };

    public abstract String cypherName();

    public abstract String csvName();

    public abstract DefaultValue fallbackValue();

    public abstract <RESULT> RESULT accept(Visitor<RESULT> visitor);

    public static ValueType fromCsvName(String csvName) {
        for (ValueType value : values()) {
            if (value == UNKNOWN) {
                continue;
            }
            if (value.csvName().equals(csvName)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unexpected value: " + csvName);
    }

    public interface Visitor<RESULT> {
        RESULT visitLong();
        RESULT visitDouble();
        RESULT visitString();
        RESULT visitLongArray();
        RESULT visitDoubleArray();
        RESULT visitFloatArray();

        default @Nullable RESULT visitUnknown() { return null; }
    }
}
