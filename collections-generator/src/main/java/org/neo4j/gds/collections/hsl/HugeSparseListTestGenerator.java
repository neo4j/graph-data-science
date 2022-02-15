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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.collections.CollectionStep;

import javax.lang.model.element.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.neo4j.gds.collections.TestGeneratorUtils.ASSERTJ_ASSERTIONS;
import static org.neo4j.gds.collections.TestGeneratorUtils.TEST_ANNOTATION;
import static org.neo4j.gds.collections.TestGeneratorUtils.defaultValue;
import static org.neo4j.gds.collections.TestGeneratorUtils.nonDefaultValue;
import static org.neo4j.gds.collections.TestGeneratorUtils.randomIndex;
import static org.neo4j.gds.collections.TestGeneratorUtils.randomValue;
import static org.neo4j.gds.collections.TestGeneratorUtils.zeroValue;

final class HugeSparseListTestGenerator implements CollectionStep.Generator<HugeSparseListValidation.Spec> {

    @Override
    public TypeSpec generate(HugeSparseListValidation.Spec spec) {
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
                .addStatement("var list = $T.of($L)", elementType, defaultValue(valueType))
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
                .addStatement("var list = $T.of($L)", elementType, defaultValue(valueType))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, nonDefaultValue(valueType))
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
                .addStatement("var list = $T.of($L)", elementType, defaultValue(valueType))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, defaultValue(valueType))
                .addStatement("list.addTo(index, value)")
                .addStatement("list.addTo(index, value)")
                .addStatement(
                    "$1T.assertThat(list.get(index)).isEqualTo(($2T) ($3L + $3L + $3L))",
                    ASSERTJ_ASSERTIONS,
                    valueType,
                    defaultValue(valueType)
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
                .addStatement("var list = $T.of($L)", elementType, zeroValue(valueType))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, defaultValue(valueType))
                .addStatement("list.addTo(index, value)")
                .addStatement("list.addTo(index, value)")
                .addStatement(
                    "$1T.assertThat(list.get(index)).isEqualTo(($2T) ($3L + $3L))",
                    ASSERTJ_ASSERTIONS,
                    valueType,
                    defaultValue(valueType)
                )
                .build())
            .build();
    }

    private static MethodSpec shouldReturnDefaultValue(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldReturnDefaultValue")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var list = $T.of($L)", elementType, defaultValue(valueType))
                .addStatement("// > PAGE_SIZE")
                .addStatement("var index = 224242")
                .addStatement("list.set(index, $N)", nonDefaultValue(valueType))
                .beginControlFlow("for (long i = 0; i < index; i++)")
                .addStatement(
                    "$T.assertThat(list.get(i)).isEqualTo($N)",
                    ASSERTJ_ASSERTIONS,
                    defaultValue(valueType)
                )
                .endControlFlow()
                .addStatement(
                    "$T.assertThat(list.get(index)).isEqualTo($N)",
                    ASSERTJ_ASSERTIONS,
                    nonDefaultValue(valueType)
                )
                .build())
            .build();
    }

    private static MethodSpec shouldHaveSaneCapacity(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldHaveSaneCapacity")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var list = $T.of($L)", elementType, defaultValue(valueType))
                .addStatement("// > PAGE_SIZE")
                .addStatement("var index = 224242")
                .addStatement("$T value = $L", valueType, nonDefaultValue(valueType))
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
                .addStatement("var list = $T.of($L)", elementType, defaultValue(valueType))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, nonDefaultValue(valueType))
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
                .addStatement("var list = $T.of($L)", elementType, defaultValue(valueType))
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
}
