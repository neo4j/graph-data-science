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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.collections.CollectionStep;

import javax.lang.model.element.Modifier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.collections.TestGeneratorUtils.ASSERTJ_ASSERTIONS;
import static org.neo4j.gds.collections.TestGeneratorUtils.TEST_ANNOTATION;
import static org.neo4j.gds.collections.TestGeneratorUtils.defaultValue;
import static org.neo4j.gds.collections.TestGeneratorUtils.nonDefaultValue;
import static org.neo4j.gds.collections.TestGeneratorUtils.randomIndex;
import static org.neo4j.gds.collections.TestGeneratorUtils.randomValue;
import static org.neo4j.gds.collections.TestGeneratorUtils.variableValue;
import static org.neo4j.gds.collections.TestGeneratorUtils.zeroValue;

final class HugeSparseArrayTestGenerator implements CollectionStep.Generator<HugeSparseArrayValidation.Spec> {

    @Override
    public TypeSpec generate(HugeSparseArrayValidation.Spec spec) {
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
        builder.addMethod(shouldSetParallel(valueType, elementType));

        return builder.build();
    }

    private static CodeBlock newBuilder(TypeName elementType, String defaultValue) {
        return CodeBlock.builder()
            .addStatement(
                "var builder = $T.builder($L)",
                elementType,
                defaultValue
            )
            .build();
    }

    private static MethodSpec shouldSetAndGet(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldSetAndGet")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .add(newBuilder(elementType, defaultValue(valueType)))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, randomValue(valueType))
                .addStatement("builder.set(index, value)")
                .addStatement("var array = builder.build()")
                .addStatement("$T.assertThat(array.get(index)).isEqualTo(value)", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }


    private static MethodSpec shouldOnlySetIfAbsent(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldOnlySetIfAbsent")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .add(newBuilder(elementType, defaultValue(valueType)))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, nonDefaultValue(valueType))
                .addStatement("$T.assertThat(builder.setIfAbsent(index, value)).isTrue()", ASSERTJ_ASSERTIONS)
                .addStatement("$T.assertThat(builder.setIfAbsent(index, value)).isFalse()", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }

    private static MethodSpec shouldAddToAndGet(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldAddToAndGet")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .add(newBuilder(elementType, defaultValue(valueType)))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, defaultValue(valueType))
                .addStatement("builder.addTo(index, value)")
                .addStatement("builder.addTo(index, value)")
                .addStatement("var array = builder.build()")
                .addStatement(
                    "$1T.assertThat(array.get(index)).isEqualTo(($2T) ($3L + $3L + $3L))",
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
                .add(newBuilder(elementType, zeroValue(valueType)))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, defaultValue(valueType))
                .addStatement("builder.addTo(index, value)")
                .addStatement("builder.addTo(index, value)")
                .addStatement("var array = builder.build()")
                .addStatement(
                    "$1T.assertThat(array.get(index)).isEqualTo(($2T) ($3L + $3L))",
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
                .add(newBuilder(elementType, defaultValue(valueType)))
                .addStatement("// > PAGE_SIZE")
                .addStatement("var index = 224242")
                .addStatement("builder.set(index, $N)", nonDefaultValue(valueType))
                .addStatement("var array = builder.build()")
                .beginControlFlow("for (long i = 0; i < index; i++)")
                .addStatement(
                    "$T.assertThat(array.get(i)).isEqualTo($N)",
                    ASSERTJ_ASSERTIONS,
                    defaultValue(valueType)
                )
                .endControlFlow()
                .addStatement(
                    "$T.assertThat(array.get(index)).isEqualTo($N)",
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
                .add(newBuilder(elementType, defaultValue(valueType)))
                .addStatement("// > PAGE_SIZE")
                .addStatement("var index = 224242")
                .addStatement("$T value = $L", valueType, nonDefaultValue(valueType))
                .addStatement("builder.set(index, value)")
                .addStatement("var array = builder.build()")
                .addStatement("$T.assertThat(array.capacity()).isGreaterThanOrEqualTo(index)", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }

    private static MethodSpec shouldReportContainsCorrectly(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldReportContainsCorrectly")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addCode(CodeBlock.builder()
                .addStatement("var random = $T.current()", ThreadLocalRandom.class)
                .add(newBuilder(elementType, defaultValue(valueType)))
                .addStatement("long index = $L", randomIndex())
                .addStatement("$T value = $L", valueType, nonDefaultValue(valueType))
                .addStatement("builder.set(index, value)")
                .addStatement("var array = builder.build()")
                .addStatement("$T.assertThat(array.contains(index)).isTrue()", ASSERTJ_ASSERTIONS)
                .addStatement("$T.assertThat(array.contains(index + 1)).isFalse()", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }

    private static MethodSpec shouldSetParallel(TypeName valueType, TypeName elementType) {
        return MethodSpec.methodBuilder("shouldSetParallel")
            .addAnnotation(TEST_ANNOTATION)
            .returns(TypeName.VOID)
            .addException(ExecutionException.class)
            .addException(InterruptedException.class)
            .addCode(CodeBlock.builder()
                .add(newBuilder(elementType, defaultValue(valueType)))
                .addStatement("var cores = $T.getRuntime().availableProcessors()", Runtime.class)
                .addStatement("var executor = $T.newFixedThreadPool(cores)", Executors.class)
                .addStatement("var batches = 10")
                .addStatement("var batchSize = 10_000")
                .addStatement("var processed = new $T(0)", AtomicLong.class)
                .addStatement(
                    "var tasks = $T.range(0, batches)\n" +
                    ".mapToObj(threadId -> ($T) () -> {\n" +
                    "   var start = processed.getAndAdd(batchSize);\n" +
                    "   var end = start + batchSize;\n" +
                    "   for (long idx = start; idx < end; idx++) {\n" +
                    "       builder.set(idx, $L);\n" +
                    "   }\n" +
                    "}).collect($T.toList());",
                    IntStream.class,
                    Runnable.class,
                    variableValue(valueType, "threadId"),
                    Collectors.class
                )
                .addStatement(
                    "var futures = tasks.stream().map(executor::submit).collect($T.toList())",
                    Collectors.class
                )
                .beginControlFlow("for (var future : futures)")
                .addStatement("future.get()")
                .endControlFlow()
                .addStatement("var array = builder.build()")
                .addStatement("long sum = 0")
                .beginControlFlow("for (long idx = 0; idx < batchSize * batches; idx++)")
                .addStatement(valueType.isPrimitive() ? "sum += array.get(idx)" : "sum += array.get(idx)[0]")
                .endControlFlow()
                .addStatement("$T.assertThat(sum).isEqualTo(450_000)", ASSERTJ_ASSERTIONS)
                .build())
            .build();
    }
}
