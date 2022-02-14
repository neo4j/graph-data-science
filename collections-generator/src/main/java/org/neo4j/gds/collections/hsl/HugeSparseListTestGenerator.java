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
package org.neo4j.gds.collections.hsl;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import javax.lang.model.element.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Map.entry;

final class HugeSparseListTestGenerator {

    // To avoid having implementation dependencies on
    // junit / assertj, we use fully qualified names instead.
    private static final ClassName ASSERTJ_ASSERTIONS = ClassName.get("org.assertj.core.api", "Assertions");
    private static final ClassName TEST_ANNOTATION = ClassName.get("org.junit.jupiter.api", "Test");

    private HugeSparseListTestGenerator() {}

    static TypeSpec generate(HugeSparseListValidation.Spec spec) {
        var className = ClassName.get(spec.rootPackage().toString(), spec.className() + "Test");
        var elementType = TypeName.get(spec.element().asType());
        var valueType = TypeName.get(spec.valueType());

        var builder = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.FINAL)
            .addOriginatingElement(spec.element());

        builder.addMethod(shouldSetAndGet(valueType, elementType));
        if (valueType.isPrimitive()) {
            builder.addMethod(shouldOnlySetIfAbsent(valueType, elementType));
            builder.addMethod(shouldAddToAndGet(valueType, elementType));
            builder.addMethod(shouldAddToAndGetWithDefaultZero(valueType, elementType));
        }
        builder.addMethod(shouldReturnDefaultValue(valueType, elementType));
        builder.addMethod(shouldHaveSaneCapacity(valueType, elementType));
        builder.addMethod(shouldReportContainsCorrectly(valueType, elementType));
        builder.addMethod(shouldReturnValuesUsingForAll(valueType, elementType));

        return builder.build();
    }

    private static MethodSpec shouldSetAndGet(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldSetAndGet")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .addStatement("var list = $T.of($L)", elementType, DEFAULT_VALUES.get(valueType))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, randomValue(valueType))
                .addStatement("list.set(index, value)")
                .addStatement("$T.assertThat(list.get(index)).isEqualTo(value)", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }


    private static MethodSpec shouldOnlySetIfAbsent(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldOnlySetIfAbsent")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .addStatement("var list = $T.of($L)", elementType, DEFAULT_VALUES.get(valueType))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, NON_DEFAULT_VALUES.get(valueType))
                .addStatement("$T.assertThat(list.setIfAbsent(index, value)).isTrue()", ASSERTJ_ASSERTIONS)
                .addStatement("$T.assertThat(list.setIfAbsent(index, value)).isFalse()", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }

    private static MethodSpec shouldAddToAndGet(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldAddToAndGet")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .addStatement("var list = $T.of($L)", elementType, DEFAULT_VALUES.get(valueType))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, DEFAULT_VALUES.get(valueType))
                .addStatement("list.addTo(index, value)")
                .addStatement("list.addTo(index, value)")
                .addStatement(
                    "$1T.assertThat(list.get(index)).isEqualTo(($2T) ($3L + $3L + $3L))",
                    ASSERTJ_ASSERTIONS,
                    valueType,
                    DEFAULT_VALUES.get(valueType)
                )
                .build())
            .build();
    }

    private static MethodSpec shouldAddToAndGetWithDefaultZero(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldAddToAndGetWithDefaultZero")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .addStatement("var list = $T.of($L)", elementType, ZERO_VALUES.get(valueType))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, DEFAULT_VALUES.get(valueType))
                .addStatement("list.addTo(index, value)")
                .addStatement("list.addTo(index, value)")
                .addStatement(
                    "$1T.assertThat(list.get(index)).isEqualTo(($2T) ($3L + $3L))",
                    ASSERTJ_ASSERTIONS,
                    valueType,
                    DEFAULT_VALUES.get(valueType)
                )
                .build())
            .build();
    }

    private static MethodSpec shouldReturnDefaultValue(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldReturnDefaultValue")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var list = $T.of($L)", elementType, DEFAULT_VALUES.get(valueType))
                .addStatement("// > PAGE_SIZE")
                .addStatement("var index = 224242")
                .addStatement("list.set(index, $N)", NON_DEFAULT_VALUES.get(valueType))
                .beginControlFlow("for (long i = 0; i < index; i++)")
                .addStatement(
                    "$T.assertThat(list.get(i)).isEqualTo($N)",
                    ASSERTJ_ASSERTIONS,
                    DEFAULT_VALUES.get(valueType)
                )
                .endControlFlow()
                .addStatement(
                    "$T.assertThat(list.get(index)).isEqualTo($N)",
                    ASSERTJ_ASSERTIONS,
                    NON_DEFAULT_VALUES.get(valueType)
                )
                .build())
            .build();
    }

    private static MethodSpec shouldHaveSaneCapacity(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldHaveSaneCapacity")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var list = $T.of($L)", elementType, DEFAULT_VALUES.get(valueType))
                .addStatement("// > PAGE_SIZE")
                .addStatement("var index = 224242")
                .addStatement("$T value = $L", valueType, NON_DEFAULT_VALUES.get(valueType))
                .addStatement("list.set(index, value)")
                .addStatement("$T.assertThat(list.capacity()).isGreaterThanOrEqualTo(index)", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }

    private static MethodSpec shouldReportContainsCorrectly(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldReportContainsCorrectly")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .addStatement("var list = $T.of($L)", elementType, DEFAULT_VALUES.get(valueType))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, NON_DEFAULT_VALUES.get(valueType))
                .addStatement("list.set(index, value)")
                .addStatement("$T.assertThat(list.contains(index)).isTrue()", ASSERTJ_ASSERTIONS)
                .addStatement("$T.assertThat(list.contains(index + 1)).isFalse()", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }

    private static MethodSpec shouldReturnValuesUsingForAll(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldReturnValuesUsingForAll")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .addStatement("var list = $T.of($L)", elementType, DEFAULT_VALUES.get(valueType))
                .addStatement(
                    "var expected = $T.of($L, $L, $L, $L, $L, $L)",
                    Map.class,
                    randomIndex(0, 4096),
                    randomValue(valueType),
                    randomIndex(4096, 8192),
                    randomValue(valueType),
                    randomIndex(8192, 8192 + 4096),
                    randomValue(valueType)
                )
                .addStatement("expected.forEach(list::set)")
                .addStatement("var actual = new $T<>()", HashMap.class)
                .addStatement("list.forAll(actual::put)")
                .addStatement("$T.assertThat(actual).isEqualTo(expected)", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }

    private static final Map<TypeName, String> ZERO_VALUES = Map.of(
        TypeName.BYTE, "(byte) 0",
        TypeName.SHORT, "(short) 0",
        TypeName.INT, "0",
        TypeName.LONG, "0L",
        TypeName.FLOAT, "0.0F",
        TypeName.DOUBLE, "0.0D"
    );

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
        entry(ArrayTypeName.of(TypeName.DOUBLE), "new double[] { 4.4D, 2.2D }")
    );

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
        entry(ArrayTypeName.of(TypeName.DOUBLE), "new double[] { 1.1D, 3.3D, 3.3D, 7.7D }")
    );

    private static String randomIndex() {
        // TODO call into other
        return "random.nextLong(0, 4096L + 1337L)";
    }

    private static String randomIndex(long from, long to) {
        return "random.nextLong(" + from + ", " + to + ")";
    }

    private static String randomValue(TypeName valueType) {
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
        } else {
            throw new IllegalArgumentException("Illegal type");
        }
    }

    private static String variableValue(TypeName valueType, String variable) {
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
        } else {
            throw new IllegalArgumentException("Illegal type");
        }
    }
}
