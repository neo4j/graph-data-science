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
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;

import java.util.Locale;
import java.util.Map;

import static java.util.Map.entry;

public final class EqualityUtils {

    public static final Map<TypeName, String> DEFAULT_VALUES = Map.of(
        TypeName.BYTE, "0",
        TypeName.SHORT, "0",
        TypeName.INT, "0",
        TypeName.LONG, "0l",
        TypeName.FLOAT, "0f",
        TypeName.DOUBLE, "0d"
    );

    public static <LHS, RHS> CodeBlock isEqual(
        TypeName typeName,
        String lhsType,
        String rhsType,
        LHS lhs,
        RHS rhs
    ) {
        return CodeBlock
            .builder()
            .add(String.format(Locale.ENGLISH, EQUAL_PREDICATES.get(typeName), lhsType, rhsType), lhs, rhs)
            .build();
    }

    public static <LHS, RHS> CodeBlock isNotEqual(
        TypeName typeName,
        String lhsType,
        String rhsType,
        LHS lhs,
        RHS rhs
    ) {
        return CodeBlock
            .builder()
            .add(String.format(Locale.ENGLISH, NOT_EQUAL_PREDICATES.get(typeName), lhsType, rhsType), lhs, rhs)
            .build();
    }

    private static final Map<TypeName, String> EQUAL_PREDICATES = Map.ofEntries(
        entry(TypeName.BYTE, "%s == %s"),
        entry(TypeName.SHORT, "%s == %s"),
        entry(TypeName.INT, "%s == %s"),
        entry(TypeName.LONG, "%s == %s"),
        entry(TypeName.FLOAT, "Float.compare(%s, %s) == 0"),
        entry(TypeName.DOUBLE, "Double.compare(%s, %s) == 0"),
        entry(ArrayTypeName.of(TypeName.BYTE), "%1$s != null && Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.SHORT), "%1$s != null && Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.INT), "%1$s != null && Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.LONG), "%1$s != null && Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.FLOAT), "%1$s != null && Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.DOUBLE), "%1$s != null && Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(ArrayTypeName.of(TypeName.BYTE)), "%1$s != null && Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(ArrayTypeName.of(TypeName.LONG)), "%1$s != null && Arrays.equals(%1$s, %2$s)")
    );

    private static final Map<TypeName, String> NOT_EQUAL_PREDICATES = Map.ofEntries(
        entry(TypeName.BYTE, "%s != %s"),
        entry(TypeName.SHORT, "%s != %s"),
        entry(TypeName.INT, "%s != %s"),
        entry(TypeName.LONG, "%s != %s"),
        entry(TypeName.FLOAT, "Float.compare(%s, %s) != 0"),
        entry(TypeName.DOUBLE, "Double.compare(%s, %s) != 0"),
        entry(ArrayTypeName.of(TypeName.BYTE), "%1$s != null && !Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.SHORT), "%1$s != null && !Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.INT), "%1$s != null && !Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.LONG), "%1$s != null && !Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.FLOAT), "%1$s != null && !Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.DOUBLE), "%1$s != null && !Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(ArrayTypeName.of(TypeName.BYTE)), "%1$s != null && !Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(ArrayTypeName.of(TypeName.LONG)), "%1$s != null && !Arrays.equals(%1$s, %2$s)")
    );

    private EqualityUtils() {}
}
