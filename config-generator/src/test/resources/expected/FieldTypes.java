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
package positive;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.processing.Generated;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class FieldTypesConfig implements FieldTypes {

    private final boolean aBoolean;

    private final byte aByte;

    private final short aShort;

    private final int anInt;

    private final long aLong;

    private final float aFloat;

    private final double aDouble;

    private final Number aNumber;

    private final String aString;

    private final Map<String, Object> aMap;

    private final List<Object> aList;

    public FieldTypesConfig(@NotNull CypherMapWrapper config) {
        this.aBoolean = config.requireBool("aBoolean");
        this.aByte = config.requireNumber("aByte").byteValue();
        this.aShort = config.requireNumber("aShort").shortValue();
        this.anInt = config.requireInt("anInt");
        this.aLong = config.requireLong("aLong");
        this.aFloat = config.requireNumber("aFloat").floatValue();
        this.aDouble = config.requireDouble("aDouble");
        this.aNumber = CypherMapWrapper.failOnNull("aNumber", config.requireNumber("aNumber"));
        this.aString = CypherMapWrapper.failOnNull("aString", config.requireString("aString"));
        this.aMap = CypherMapWrapper.failOnNull("aMap", config.requireChecked("aMap", Map.class));
        this.aList = CypherMapWrapper.failOnNull("aList", config.requireChecked("aList", List.class));
    }

    public static Collection<String> configKeys() {
        return Arrays.asList(
            "aBoolean",
            "aByte",
            "aShort",
            "anInt",
            "aLong",
            "aFloat",
            "aDouble",
            "aNumber",
            "aString",
            "aMap",
            "aList"
        );
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
        return this.aLong
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
}
