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
package org.neo4j.gds.collections.haa;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.gds.mem.MemoryUsage;

import javax.lang.model.element.Modifier;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static org.neo4j.gds.collections.haa.HugeAtomicArrayGenerator.DEFAULT_VALUE_METHOD;
import static org.neo4j.gds.collections.haa.HugeAtomicArrayGenerator.PAGE_UTIL;
import static org.neo4j.gds.collections.haa.HugeAtomicArrayGenerator.arrayHandleField;
import static org.neo4j.gds.collections.haa.HugeAtomicArrayGenerator.valueArrayType;
import static org.neo4j.gds.collections.haa.PagedArrayBuilder.PAGED_CLASS_NAME;

final class SingleArrayBuilder {

    static final String SINLGE_CLASS_NAME = "Single";

    private SingleArrayBuilder() {}

    static TypeSpec builder(
        TypeName interfaceType,
        ClassName baseClassName,
        TypeName valueType,
        TypeName unaryOperatorType,
        TypeName pageCreatorType
    ) {
        var builder = TypeSpec.classBuilder(SINLGE_CLASS_NAME)
            .addModifiers(Modifier.STATIC, Modifier.FINAL)
            .superclass(baseClassName)
            .addSuperinterface(interfaceType);

        // instance fields
        var arrayHandle = arrayHandleField(valueType);
        var size = sizeField();
        var page = pageField(valueType);
        builder.addField(arrayHandle);
        builder.addField(size);
        builder.addField(page);

        builder.addMethod(ofMethod(valueType, interfaceType, pageCreatorType));
        builder.addMethod(constructor(valueType));

        // static methods
        builder.addMethod(memoryEstimationMethod(valueType));

        // instance methods
        builder.addMethod(getMethod(valueType, arrayHandle, page));
        builder.addMethod(getAndAddMethod(valueType, arrayHandle));
        builder.addMethod(getAndReplaceMethod(valueType, arrayHandle));
        builder.addMethod(setMethod(valueType, arrayHandle, page));
        builder.addMethod(updateMethod(valueType, unaryOperatorType, arrayHandle, page));
        builder.addMethod(compareAndSetMethod(valueType, arrayHandle, page));
        builder.addMethod(compareAndExchangeMethod(valueType, arrayHandle, page));
        builder.addMethod(newCursorMethod(valueType, page));
        builder.addMethod(sizeMethod(size));
        builder.addMethod(sizeOfMethod(valueType));
        builder.addMethod(setAllMethod(valueType, page));
        builder.addMethod(releaseMethod(page, valueType));
        builder.addMethod(copyToMethod(interfaceType, valueType, page));

        return builder.build();
    }

    private static FieldSpec sizeField() {
        return FieldSpec
            .builder(TypeName.INT, "size", Modifier.PRIVATE, Modifier.FINAL)
            .build();
    }

    private static FieldSpec pageField(TypeName valueType) {
        return FieldSpec
            .builder(valueArrayType(valueType), "page", Modifier.PRIVATE)
            .build();
    }

    private static MethodSpec memoryEstimationMethod(TypeName valueType) {
        return MethodSpec.methodBuilder("memoryEstimation")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(TypeName.LONG, "size")
            .returns(TypeName.LONG)
            .addStatement(
                "return $T.sizeOf$NArray((int) size)",
                MemoryUsage.class,
                StringUtils.capitalize(valueType.toString())
            )
            .build();
    }

    private static MethodSpec ofMethod(TypeName valueType, TypeName interfaceType, TypeName pageCreatorType) {
        return MethodSpec.methodBuilder("of")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(TypeName.LONG, "size")
            .addParameter(pageCreatorType, "pageCreator")
            .returns(interfaceType)
            .addStatement("assert size <= $T.MAX_ARRAY_LENGTH", PAGE_UTIL)
            .addStatement("int intSize = (int) size")
            .addStatement("$T page = new $T[intSize]", valueArrayType(valueType), valueType)
            .addStatement("pageCreator.fillPage(page, 0)")
            .addStatement("return new $N(intSize, page)", SingleArrayBuilder.SINLGE_CLASS_NAME)
            .build();
    }


    private static MethodSpec constructor(TypeName valueType) {
        return MethodSpec.constructorBuilder()
            .addParameter(TypeName.INT, "size")
            .addParameter(valueArrayType(valueType), "page")
            .addStatement("this.size = size")
            .addStatement("this.page = page")
            .build();
    }

    private static MethodSpec getMethod(
        TypeName valueType,
        FieldSpec arrayHandle,
        FieldSpec page
    ) {
        return MethodSpec.methodBuilder("get")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .returns(valueType)
            .addStatement("return ($T) $N.getVolatile($N, (int) index)", valueType, arrayHandle, page)
            .build();
    }

    private static MethodSpec getAndAddMethod(
        TypeName valueType,
        FieldSpec arrayHandle
    ) {
        return MethodSpec
            .methodBuilder("getAndAdd")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .addParameter(valueType, "delta")
            .returns(valueType)
            .addStatement("$1T prev = ($1T) $2N.getAcquire(page, (int) index)", valueType, arrayHandle)
            .addCode(CodeBlock.builder()
                .beginControlFlow("while (true)")
                .addStatement("$1T next = ($1T) (prev + delta)", valueType)
                .addStatement(
                    "$1T current = ($1T) $2N.compareAndExchangeRelease(page, (int) index, prev, next)",
                    valueType,
                    arrayHandle
                )
                .beginControlFlow("if ($T.compare(prev, current) == 0)", valueType.box())
                .addStatement("return prev")
                .endControlFlow()
                .addStatement("prev = current")
                .endControlFlow()
                .build())
            .build();
    }

    private static MethodSpec getAndReplaceMethod(
        TypeName valueType,
        FieldSpec arrayHandle
    ) {
        return MethodSpec.methodBuilder("getAndReplace")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .addParameter(valueType, "value")
            .returns(valueType)
            .addStatement("$1T prev = ($1T) $2N.getAcquire(page, (int) index)", valueType, arrayHandle)
            .addCode(CodeBlock.builder().beginControlFlow("while (true)")
                .addStatement(
                    "$1T current = ($1T) $2N.compareAndExchangeRelease(page, (int) index, prev, value)",
                    valueType,
                    arrayHandle
                )
                .beginControlFlow("if ($T.compare(prev, current) == 0)", valueType.box())
                .addStatement("return current")
                .endControlFlow()
                .addStatement("prev = current")
                .endControlFlow()
                .build())
            .build();
    }

    private static MethodSpec setMethod(TypeName valueType, FieldSpec arrayHandle, FieldSpec page) {
        return MethodSpec.methodBuilder("set")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .addParameter(valueType, "value")
            .returns(TypeName.VOID)
            .addStatement("$N.setVolatile($N, (int) index, value)", arrayHandle, page)
            .build();
    }

    private static MethodSpec compareAndSetMethod(TypeName valueType, FieldSpec arrayHandle, FieldSpec page) {
        return MethodSpec.methodBuilder("compareAndSet")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .addParameter(valueType, "expected")
            .addParameter(valueType, "update")
            .returns(TypeName.BOOLEAN)
            .addStatement("return $N.compareAndSet($N, (int) index, expected, update)", arrayHandle, page)
            .build();
    }

    private static MethodSpec compareAndExchangeMethod(TypeName valueType, FieldSpec arrayHandle, FieldSpec page) {
        return MethodSpec.methodBuilder("compareAndExchange")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .addParameter(valueType, "expected")
            .addParameter(valueType, "update")
            .returns(valueType)
            .addStatement(
                "return ($T) $N.compareAndExchange($N, (int) index, expected, update)",
                valueType,
                arrayHandle,
                page
            )
            .build();
    }

    private static MethodSpec updateMethod(TypeName valueType, TypeName unaryOperatorType, FieldSpec arrayHandle, FieldSpec page) {
        return MethodSpec.methodBuilder("update")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .addParameter(unaryOperatorType, "updateFunction")
            .returns(TypeName.VOID)
            .addStatement("$1T prev = ($1T) $2N.getAcquire($3N, (int) index)", valueType, arrayHandle, page)
            .addCode(CodeBlock.builder().beginControlFlow("while (true)")
                .addStatement("$T next = updateFunction.apply(prev)", valueType)
                .addStatement(
                    "$1T current = ($1T) $2N.compareAndExchangeRelease($3N, (int) index, prev, next)",
                    valueType,
                    arrayHandle,
                    page
                )
                .beginControlFlow("if ($T.compare(prev, current) == 0)", valueType.box())
                .addStatement("return")
                .endControlFlow()
                .addStatement("prev = current")
                .endControlFlow()
                .build())
            .build();
    }


    private static MethodSpec newCursorMethod(TypeName valueType, FieldSpec page) {
        ClassName hugeCursorType = ClassName.get("org.neo4j.gds.collections.cursor", "HugeCursor");
        ParameterizedTypeName hugeCursorGenericType = ParameterizedTypeName.get(
            hugeCursorType,
            valueArrayType(valueType)
        );

        return MethodSpec.methodBuilder("newCursor")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(hugeCursorGenericType)
            .addStatement("return new $T.SinglePageCursor<>($N)", hugeCursorType, page)
            .build();
    }

    private static MethodSpec sizeMethod(FieldSpec size) {
        return MethodSpec.methodBuilder("size")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.LONG)
            .addStatement("return $N", size)
            .build();
    }

    private static MethodSpec sizeOfMethod(TypeName valueType) {
        return MethodSpec.methodBuilder("sizeOf")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.LONG)
            .addStatement("return $T.sizeOf$NArray((int) size)",
                MemoryUsage.class,
                StringUtils.capitalize(valueType.toString()))
            .build();
    }

    private static MethodSpec setAllMethod(TypeName valueType, FieldSpec page) {
        return MethodSpec.methodBuilder("setAll")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(valueType, "value")
            .returns(TypeName.VOID)
            .addStatement("$T.fill($N, value)", ClassName.get(Arrays.class), page)
            .addStatement("$T.storeStoreFence()", ClassName.get(VarHandle.class))
            .build();
    }

    private static MethodSpec releaseMethod(FieldSpec page, TypeName valueType) {
        return MethodSpec.methodBuilder("release")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.LONG)
            .beginControlFlow("if ($N != null)", page)
            .addStatement("$N = null", page)
            .addStatement("return $T.sizeOf$NArray((int) size)",
                MemoryUsage.class,
                StringUtils.capitalize(valueType.toString()))
            .endControlFlow()
            .addStatement("return 0L")
            .build();
    }

    private static MethodSpec copyToMethod(
        TypeName interfaceType,
        TypeName valueType,
        FieldSpec page
    ) {
        return MethodSpec.methodBuilder("copyTo")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(interfaceType, "dest")
            .addParameter(TypeName.LONG, "length")
            .returns(TypeName.VOID)
            .addStatement("$T defaultValue = $N()", valueType, DEFAULT_VALUE_METHOD)
            .addCode(CodeBlock.builder()
                .beginControlFlow("if (dest instanceof $N)", SINLGE_CLASS_NAME)
                .addStatement("$1N dst = ($1N) dest", SINLGE_CLASS_NAME)
                .addStatement("$1T.arraycopy($2N, 0, dst.$2N, 0, (int) length)", System.class, page)
                .addStatement("$1T.fill(dst.$2N, (int) length, dst.size, defaultValue)", Arrays.class, page)
                .endControlFlow()
                .build())
            .addCode(CodeBlock.builder()
                .beginControlFlow("else if (dest instanceof $N)", PAGED_CLASS_NAME)
                .addStatement("$1N dst = ($1N) dest", PAGED_CLASS_NAME)
                .addStatement("int start = 0")
                .addStatement("int remaining = (int) length")
                .beginControlFlow("if (length > dst.size())")
                .addStatement("length = dst.size()")
                .endControlFlow()
                .add(CodeBlock.builder()
                    .beginControlFlow("for($T dstPage: dst.pages)", valueArrayType(valueType))
                    .addStatement("int toCopy = $T.min(remaining, dstPage.length)", Math.class)
                    .beginControlFlow("if (toCopy == 0)")
                    .addStatement("$T.fill(page, defaultValue)", Arrays.class)
                    .endControlFlow()
                    .beginControlFlow("else")
                    .addStatement("$1T.arraycopy($2N, start, dstPage, 0, toCopy)", System.class, page)
                    .beginControlFlow("if (toCopy < dstPage.length)")
                    .addStatement("$1T.fill(dstPage, toCopy, dstPage.length, defaultValue)", Arrays.class)
                    .endControlFlow()
                    .endControlFlow()
                    .endControlFlow()
                    .build())
                .build())
            .endControlFlow()
            .beginControlFlow("else")
            .addStatement(
                "throw new $T(\"Can handle only the known implementations of Single and Paged versions.\")",
                RuntimeException.class
            )
            .endControlFlow()
            .build();
    }
}
