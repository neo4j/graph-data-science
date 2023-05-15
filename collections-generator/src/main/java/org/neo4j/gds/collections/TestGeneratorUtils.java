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
package org.neo4j.gds.collections;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

import java.util.Map;

import static java.util.Map.entry;

public final class TestGeneratorUtils {

    // To avoid having implementation dependencies on
    // junit / assertj, we use fully qualified names instead.
    public static final ClassName ASSERTJ_ASSERTIONS = ClassName.get("org.assertj.core.api", "Assertions");
    public static final ClassName TEST_ANNOTATION = ClassName.get("org.junit.jupiter.api", "Test");


    public static String zeroValue(TypeName typeName) {
        return ZERO_VALUES.get(typeName);
    }

    private static final Map<TypeName, String> ZERO_VALUES = Map.of(
        TypeName.BYTE, "(byte) 0",
        TypeName.SHORT, "(short) 0",
        TypeName.INT, "0",
        TypeName.LONG, "0L",
        TypeName.FLOAT, "0.0F",
        TypeName.DOUBLE, "0.0D"
    );

    public static String defaultValue(TypeName typeName) {
        return DEFAULT_VALUES.get(typeName);
    }

    private static final Map<TypeName, String> DEFAULT_VALUES = Map.ofEntries(
        entry(TypeName.BYTE, "(byte) 42"),
        entry(TypeName.SHORT, "(short) 42"),
        entry(TypeName.INT, "42"),
        entry(TypeName.LONG, "42L"),
        entry(TypeName.FLOAT, "42.1337F"),
        entry(TypeName.DOUBLE, "42.1337D"),
        entry(ArrayTypeName.of(TypeName.BYTE), "new byte[] { 4, 2 }"),
        entry(ArrayTypeName.of(TypeName.SHORT), "new short[] { 4, 2 }"),
        entry(ArrayTypeName.of(TypeName.INT), "new int[] { 4, 2 }"),
        entry(ArrayTypeName.of(TypeName.LONG), "new long[] { 4, 2 }"),
        entry(ArrayTypeName.of(TypeName.FLOAT), "new float[] { 4.4F, 2.2F }"),
        entry(ArrayTypeName.of(TypeName.DOUBLE), "new double[] { 4.4D, 2.2D }"),
        entry(ArrayTypeName.of(ArrayTypeName.of(TypeName.BYTE)), "new byte[][] { new byte[] { 4 }, new byte[] { 2 } }")
    );

    public static String nonDefaultValue(TypeName typeName) {
        return NON_DEFAULT_VALUES.get(typeName);
    }

    private static final Map<TypeName, String> NON_DEFAULT_VALUES = Map.ofEntries(
        entry(TypeName.BYTE, "(byte) 1337"),
        entry(TypeName.SHORT, "(short) 1337"),
        entry(TypeName.INT, "1337"),
        entry(TypeName.LONG, "1337L"),
        entry(TypeName.FLOAT, "1337.42F"),
        entry(TypeName.DOUBLE, "1337.42D"),
        entry(ArrayTypeName.of(TypeName.BYTE), "new byte[] { 1, 3, 3, 7 }"),
        entry(ArrayTypeName.of(TypeName.SHORT), "new short[] { 1, 3, 3, 7 }"),
        entry(ArrayTypeName.of(TypeName.INT), "new int[] { 1, 3, 3, 7 }"),
        entry(ArrayTypeName.of(TypeName.LONG), "new long[] { 1, 3, 3, 7 }"),
        entry(ArrayTypeName.of(TypeName.FLOAT), "new float[] { 1.1F, 3.3F, 3.3F, 7.7F }"),
        entry(ArrayTypeName.of(TypeName.DOUBLE), "new double[] { 1.1D, 3.3D, 3.3D, 7.7D }"),
        entry(
            ArrayTypeName.of(ArrayTypeName.of(TypeName.BYTE)),
            "new byte[][] { new byte[] {1, 3}, new byte[] {3, 7} }"
        )
    );

    public static String randomIndex() {
        return randomIndex(0, 4096L + 1337L);
    }

    public static String randomIndex(long from, long to) {
        return "random.nextLong(" + from + ", " + to + ")";
    }

    public static String randomValue(TypeName valueType) {
        if (valueType == TypeName.BYTE) {
            return "(byte) random.nextInt()";
        } else if (valueType == TypeName.SHORT) {
            return "(short) random.nextInt()";
        } else if (valueType == TypeName.INT) {
            return "random.nextInt()";
        } else if (valueType == TypeName.LONG) {
            return "random.nextLong()";
        } else if (valueType == TypeName.FLOAT) {
            return "random.nextFloat()";
        } else if (valueType == TypeName.DOUBLE) {
            return "random.nextFloat()";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.BYTE))) {
            return "new byte[] { " + randomValue(TypeName.BYTE) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.SHORT))) {
            return "new short[] { " + randomValue(TypeName.SHORT) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.INT))) {
            return "new int[] { " + randomValue(TypeName.INT) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.LONG))) {
            return "new long[] { " + randomValue(TypeName.LONG) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.FLOAT))) {
            return "new float[] { " + randomValue(TypeName.FLOAT) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.DOUBLE))) {
            return "new double[] { " + randomValue(TypeName.DOUBLE) + " }";
        } else if (valueType.equals(ArrayTypeName.of(ArrayTypeName.of(TypeName.BYTE)))) {
            return "new byte[][] { new byte[] { " + randomValue(TypeName.BYTE) + " } }";
        } else {
            throw new IllegalArgumentException("Illegal type");
        }
    }

    public static String variableValue(TypeName valueType, String variable) {
        if (valueType == TypeName.BYTE) {
            return "(byte) " + variable;
        } else if (valueType == TypeName.SHORT) {
            return "(short) " + variable;
        } else if (valueType == TypeName.INT) {
            return "(int) " + variable;
        } else if (valueType == TypeName.LONG) {
            return "(long) " + variable;
        } else if (valueType == TypeName.FLOAT) {
            return "(float) " + variable;
        } else if (valueType == TypeName.DOUBLE) {
            return "(double) " + variable;
        } else if (valueType.equals(ArrayTypeName.of(TypeName.BYTE))) {
            return "new byte[] { " + variableValue(TypeName.BYTE, variable) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.SHORT))) {
            return "new short[] { " + variableValue(TypeName.SHORT, variable) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.INT))) {
            return "new int[] { " + variableValue(TypeName.INT, variable) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.LONG))) {
            return "new long[] { " + variableValue(TypeName.LONG, variable) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.FLOAT))) {
            return "new float[] { " + variableValue(TypeName.FLOAT, variable) + " }";
        } else if (valueType.equals(ArrayTypeName.of(TypeName.DOUBLE))) {
            return "new double[] { " + variableValue(TypeName.DOUBLE, variable) + " }";
        } else if (valueType.equals(ArrayTypeName.of(ArrayTypeName.of(TypeName.BYTE)))) {
            return "new byte[][] { new byte[] { " + variableValue(TypeName.BYTE, variable) + " } }";
        } else {
            throw new IllegalArgumentException("Illegal type");
        }
    }

    private TestGeneratorUtils() {}
}
