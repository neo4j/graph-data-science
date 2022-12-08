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
package positive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class FieldTypesConfig implements FieldTypes {
    private boolean aBoolean;

    private byte aByte;

    private short aShort;

    private int anInt;

    private long aLong;

    private float aFloat;

    private double aDouble;

    private Number aNumber;

    private String aString;

    private Map<String, Object> aMap;

    private List<Object> aList;

    private Optional<String> anOptional;

    public FieldTypesConfig(@NotNull CypherMapAccess config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.aBoolean = config.requireBool("aBoolean");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aByte = config.requireNumber("aByte").byteValue();
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aShort = config.requireNumber("aShort").shortValue();
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.anInt = config.requireInt("anInt");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aLong = config.requireLong("aLong");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aFloat = config.requireNumber("aFloat").floatValue();
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aDouble = config.requireDouble("aDouble");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aNumber = CypherMapAccess.failOnNull("aNumber", config.requireNumber("aNumber"));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aString = CypherMapAccess.failOnNull("aString", config.requireString("aString"));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aMap = CypherMapAccess.failOnNull("aMap", config.requireChecked("aMap", Map.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aList = CypherMapAccess.failOnNull("aList", config.requireChecked("aList", List.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.anOptional = CypherMapAccess.failOnNull("anOptional", config.getOptional("anOptional", String.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        if (!errors.isEmpty()) {
            if (errors.size() == 1) {
                throw errors.get(0);
            } else {
                String combinedErrorMsg = errors
                    .stream()
                    .map(IllegalArgumentException::getMessage)
                    .collect(Collectors.joining(System.lineSeparator() + "\t\t\t\t",
                        "Multiple errors in configuration arguments:" + System.lineSeparator() + "\t\t\t\t",
                        ""
                    ));
                IllegalArgumentException combinedError = new IllegalArgumentException(combinedErrorMsg);
                errors.forEach(error -> combinedError.addSuppressed(error));
                throw combinedError;
            }
        }
    }

    @Override
    public boolean aBoolean() {
        return this.aBoolean;
    }

    @Override
    public byte aByte() {
        return this.aByte;
    }

    @Override
    public short aShort() {
        return this.aShort;
    }

    @Override
    public int anInt() {
        return this.anInt;
    }

    @Override
    public long aLong() {
        return this.aLong;
    }

    @Override
    public float aFloat() {
        return this.aFloat;
    }

    @Override
    public double aDouble() {
        return this.aDouble;
    }

    @Override
    public Number aNumber() {
        return this.aNumber;
    }

    @Override
    public String aString() {
        return this.aString;
    }

    @Override
    public Map<String, Object> aMap() {
        return this.aMap;
    }

    @Override
    public List<Object> aList() {
        return this.aList;
    }

    @Override
    public Optional<String> anOptional() {
        return this.anOptional;
    }

    public static FieldTypesConfig.Builder builder() {
        return new FieldTypesConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static FieldTypesConfig.Builder from(FieldTypes baseConfig) {
            var builder = new FieldTypesConfig.Builder();
            builder.aBoolean(baseConfig.aBoolean());
            builder.aByte(baseConfig.aByte());
            builder.aShort(baseConfig.aShort());
            builder.anInt(baseConfig.anInt());
            builder.aLong(baseConfig.aLong());
            builder.aFloat(baseConfig.aFloat());
            builder.aDouble(baseConfig.aDouble());
            builder.aNumber(baseConfig.aNumber());
            builder.aString(baseConfig.aString());
            builder.aMap(baseConfig.aMap());
            builder.aList(baseConfig.aList());
            builder.anOptional(baseConfig.anOptional());
            return builder;
        }

        public FieldTypesConfig.Builder aBoolean(boolean aBoolean) {
            this.config.put("aBoolean", aBoolean);
            return this;
        }

        public FieldTypesConfig.Builder aByte(byte aByte) {
            this.config.put("aByte", aByte);
            return this;
        }

        public FieldTypesConfig.Builder aShort(short aShort) {
            this.config.put("aShort", aShort);
            return this;
        }

        public FieldTypesConfig.Builder anInt(int anInt) {
            this.config.put("anInt", anInt);
            return this;
        }

        public FieldTypesConfig.Builder aLong(long aLong) {
            this.config.put("aLong", aLong);
            return this;
        }

        public FieldTypesConfig.Builder aFloat(float aFloat) {
            this.config.put("aFloat", aFloat);
            return this;
        }

        public FieldTypesConfig.Builder aDouble(double aDouble) {
            this.config.put("aDouble", aDouble);
            return this;
        }

        public FieldTypesConfig.Builder aNumber(Number aNumber) {
            this.config.put("aNumber", aNumber);
            return this;
        }

        public FieldTypesConfig.Builder aString(String aString) {
            this.config.put("aString", aString);
            return this;
        }

        public FieldTypesConfig.Builder aMap(Map<String, Object> aMap) {
            this.config.put("aMap", aMap);
            return this;
        }

        public FieldTypesConfig.Builder aList(List<Object> aList) {
            this.config.put("aList", aList);
            return this;
        }

        public FieldTypesConfig.Builder anOptional(String anOptional) {
            this.config.put("anOptional", anOptional);
            return this;
        }

        public FieldTypesConfig.Builder anOptional(Optional<String> anOptional) {
            anOptional.ifPresent(actualanOptional -> this.config.put("anOptional", actualanOptional));
            return this;
        }

        public FieldTypes build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new FieldTypesConfig(config);
        }
    }
}
