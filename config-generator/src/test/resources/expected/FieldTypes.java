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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;

import org.jetbrains.annotations.NotNull;
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

    public FieldTypesConfig(@NotNull CypherMapWrapper config) {
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
            this.aNumber = CypherMapWrapper.failOnNull("aNumber", config.requireNumber("aNumber"));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aString = CypherMapWrapper.failOnNull("aString", config.requireString("aString"));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aMap = CypherMapWrapper.failOnNull("aMap", config.requireChecked("aMap", Map.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.aList = CypherMapWrapper.failOnNull("aList", config.requireChecked("aList", List.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.anOptional = CypherMapWrapper.failOnNull("anOptional", config.getOptional("anOptional", String.class));
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
}
