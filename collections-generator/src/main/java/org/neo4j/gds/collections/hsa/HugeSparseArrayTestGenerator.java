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
package org.neo4j.gds.collections.hsa;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.lang.model.element.Modifier;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Map.entry;

final class HugeSparseArrayTestGenerator {

    private HugeSparseArrayTestGenerator() {}

    static TypeSpec generate(HugeSparseArrayValidation.Spec spec) {
        var className = ClassName.get(spec.rootPackage().toString(), spec.className() + "Test");
        var elementType = TypeName.get(spec.element().asType());
        var valueType = TypeName.get(spec.valueType());
        var builderType = TypeName.get(spec.builderType());

        var builder = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.FINAL)
            .addOriginatingElement(spec.element());

        builder.addMethod(addShouldSetAndGet(valueType, elementType));
        if (valueType.isPrimitive()) {
            builder.addMethod(addShouldOnlySetIfAbsent(valueType, elementType));
        }
        builder.addMethod(addShouldReturnDefaultValue(valueType, elementType));

        return builder.build();
    }

    private static final Map<TypeName, String> DEFAULT_VALUES = Map.ofEntries(
        entry(TypeName.BYTE, "(byte) 42"),
        entry(TypeName.SHORT, "(short) 42"),
        entry(TypeName.INT, "42"),
        entry(TypeName.LONG, "42L"),
        entry(TypeName.FLOAT, "42.1337F"),
        entry(TypeName.DOUBLE, "42.1337D"),
        entry(ArrayTypeName.of(TypeName.BYTE), "new byte[] { 1, 3, 3, 7 }"),
        entry(ArrayTypeName.of(TypeName.SHORT), "new short[] { 1, 3, 3, 7 }"),
        entry(ArrayTypeName.of(TypeName.INT), "new int[] { 1, 3, 3, 7 }"),
        entry(ArrayTypeName.of(TypeName.LONG), "new long[] { 1, 3, 3, 7 }"),
        entry(ArrayTypeName.of(TypeName.FLOAT), "new float[] { 1.1F, 3.3F, 3.3F, 7.7F }"),
        entry(ArrayTypeName.of(TypeName.DOUBLE), "new double[] { 1.1D, 3.3D, 3.3D, 7.7D }")
    );

    private static final Map<TypeName, String> NON_DEFAULT_VALUES = Map.ofEntries(
        entry(TypeName.BYTE, "(byte) 1337"),
        entry(TypeName.SHORT, "(short) 1337"),
        entry(TypeName.INT, "1337"),
        entry(TypeName.LONG, "1337L"),
        entry(TypeName.FLOAT, "1337.42F"),
        entry(TypeName.DOUBLE, "1337.42D"),
        entry(ArrayTypeName.of(TypeName.BYTE), "new byte[] { 4, 2 }"),
        entry(ArrayTypeName.of(TypeName.SHORT), "new short[] { 4, 2 }"),
        entry(ArrayTypeName.of(TypeName.INT), "new int[] { 4, 2 }"),
        entry(ArrayTypeName.of(TypeName.LONG), "new long[] { 4, 2 }"),
        entry(ArrayTypeName.of(TypeName.FLOAT), "new float[] { 4.4F, 2.2F }"),
        entry(ArrayTypeName.of(TypeName.DOUBLE), "new double[] { 4.4D, 2.2D }")
    );

    private static String randomIndex() {
        return "random.nextLong(0, 4096L + 1337L)";
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

    private static MethodSpec addShouldSetAndGet(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldSetAndGet")
            .addAnnotation(Test.class)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .addStatement(
                    "var builder = $T.growingBuilder($L, (__) -> {})",
                    elementType,
                    DEFAULT_VALUES.get(valueType)
                )
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, randomValue(valueType))
                .addStatement("builder.set(index, value)")
                .addStatement("var array = builder.build()")
                .addStatement("$T.assertThat(array.get(index)).isEqualTo(value)", Assertions.class)
                .build())
            .build();
    }

    private static MethodSpec addShouldOnlySetIfAbsent(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldOnlySetIfAbsent")
            .addAnnotation(Test.class)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .addStatement(
                    "var builder = $T.growingBuilder($L, (__) -> {})",
                    elementType,
                    DEFAULT_VALUES.get(valueType)
                )
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, NON_DEFAULT_VALUES.get(valueType))
                .addStatement("$T.assertThat(builder.setIfAbsent(index, value)).isTrue()", Assertions.class)
                .addStatement("$T.assertThat(builder.setIfAbsent(index, value)).isFalse()", Assertions.class)
                .build())
            .build();
    }

    private static MethodSpec addShouldReturnDefaultValue(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldReturnDefaultValue")
            .addAnnotation(Test.class)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement(
                    "var builder = $T.growingBuilder($L, (__) -> {})",
                    elementType,
                    DEFAULT_VALUES.get(valueType)
                )
                .addStatement("// > PAGE_SIZE")
                .addStatement("var index = 224242")
                .addStatement("builder.set(index, $N)", NON_DEFAULT_VALUES.get(valueType))
                .addStatement("var array = builder.build()")
                .beginControlFlow("for (long i = 0; i < index; i++)")
                .addStatement(
                    "$T.assertThat(array.get(i)).isEqualTo($N)",
                    Assertions.class,
                    DEFAULT_VALUES.get(valueType)
                )
                .endControlFlow()
                .addStatement(
                    "$T.assertThat(array.get(index)).isEqualTo($N)",
                    Assertions.class,
                    NON_DEFAULT_VALUES.get(valueType)
                )
                .build())
            .build();
    }
}
