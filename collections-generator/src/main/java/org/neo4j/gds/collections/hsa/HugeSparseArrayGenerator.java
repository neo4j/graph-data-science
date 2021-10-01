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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import javax.annotation.processing.Generated;
import javax.lang.model.element.Modifier;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

final class HugeSparseArrayGenerator {

    private HugeSparseArrayGenerator() {}

    static TypeSpec generate(HugeSparseArrayValidation.Spec spec) {
        var className = ClassName.get(spec.rootPackage().toString(), spec.className());
        var elementType = TypeName.get(spec.element().asType());
        var valueType = TypeName.get(spec.valueType());
        var builderType = TypeName.get(spec.builderType());

        var builder = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.FINAL)
            .addSuperinterface(elementType)
            .addOriginatingElement(spec.element());

        // class annotation
        builder.addAnnotation(generatedAnnotation());

        // class fields
        var pageShift = pageShiftField(spec.pageShift());
        var pageSize = pageSizeField(pageShift);
        var pageMask = pageMaskField(pageSize);
        builder.addField(pageShift);
        builder.addField(pageSize);
        builder.addField(pageMask);

        // instance fields
        var capacity = capacityField();
        var pages = pagesField(valueType);
        var defaultValue = defaultValueField(valueType);
        builder.addField(capacity);
        builder.addField(pages);
        builder.addField(defaultValue);

        // static methods
        var pageIndex = pageIndexMethod(pageShift);
        var indexInPage = indexInPageMethod(pageShift);
        builder.addMethod(pageIndex);
        builder.addMethod(indexInPage);

        // constructor
        builder.addMethod(constructor(valueType));

        // instance methods
        builder.addMethod(capacityMethod(capacity));
        builder.addMethod(getMethod(valueType, pages, pageIndex, indexInPage, defaultValue));
        builder.addMethod(containsMethod(valueType, pages, pageIndex, indexInPage, defaultValue));

        // GrowingBuilder
        builder.addType(GrowingBuilderGenerator.growingBuilder(elementType, builderType, valueType));

        return builder.build();
    }

    private static TypeName valueArrayType(TypeName valueType) {
        return ArrayTypeName.of(ArrayTypeName.of(valueType));
    }

    private static AnnotationSpec generatedAnnotation() {
        return AnnotationSpec.builder(Generated.class)
            .addMember("value", "$S", HugeSparseArrayGenerator.class.getCanonicalName())
            .build();
    }

    private static FieldSpec pageShiftField(int pageShift) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_SHIFT", Modifier.STATIC, Modifier.PRIVATE, Modifier.FINAL)
            .initializer("$L", pageShift)
            .build();
    }

    private static FieldSpec pageSizeField(FieldSpec pageShiftField) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_SIZE", Modifier.STATIC, Modifier.PRIVATE, Modifier.FINAL)
            .initializer("1 << $N", pageShiftField)
            .build();
    }

    private static FieldSpec pageMaskField(FieldSpec pageSizeField) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_MASK", Modifier.STATIC, Modifier.PRIVATE, Modifier.FINAL)
            .initializer("$N - 1", pageSizeField)
            .build();
    }

    private static FieldSpec capacityField() {
        return FieldSpec
            .builder(TypeName.LONG, "capacity", Modifier.PRIVATE, Modifier.FINAL)
            .build();
    }

    private static FieldSpec pagesField(TypeName valueType) {
        return FieldSpec
            .builder(valueArrayType(valueType), "pages", Modifier.PRIVATE, Modifier.FINAL)
            .build();
    }

    private static FieldSpec defaultValueField(TypeName valueType) {
        return FieldSpec
            .builder(valueType, "defaultValue", Modifier.PRIVATE, Modifier.FINAL)
            .build();
    }

    private static MethodSpec constructor(TypeName valueType) {
        return MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PRIVATE)
            .addParameter(TypeName.LONG, "capacity")
            .addParameter(valueArrayType(valueType), "pages")
            .addParameter(valueType, "defaultValue")
            .addStatement("this.capacity = capacity")
            .addStatement("this.pages = pages")
            .addStatement("this.defaultValue = defaultValue")
            .build();
    }

    private static MethodSpec pageIndexMethod(FieldSpec pageShift) {
        return MethodSpec.methodBuilder("pageIndex")
            .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
            .addParameter(TypeName.LONG, "index")
            .returns(TypeName.INT)
            .addStatement("return (int) (index >>> $N)", pageShift)
            .build();
    }

    private static MethodSpec indexInPageMethod(FieldSpec pageMask) {
        return MethodSpec.methodBuilder("indexInPage")
            .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
            .addParameter(TypeName.LONG, "index")
            .returns(TypeName.INT)
            .addStatement("return (int) (index & $N)", pageMask)
            .build();
    }

    private static MethodSpec capacityMethod(FieldSpec capacityField) {
        return MethodSpec.methodBuilder("capacity")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.LONG)
            .addStatement("return $N", capacityField)
            .build();
    }

    private static MethodSpec getMethod(
        TypeName valueType,
        FieldSpec pages,
        MethodSpec pageIndex,
        MethodSpec indexInPage,
        FieldSpec defaultValue
    ) {
        return MethodSpec.methodBuilder("get")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .returns(valueType)
            .addCode(CodeBlock.builder()
                .addStatement("int pageIndex = $N(index)", pageIndex)
                .addStatement("int indexInPage = $N(index)", indexInPage)
                .beginControlFlow("if (pageIndex < $N.length)", pages)
                .addStatement("$T[] page = $N[pageIndex]", valueType, pages)
                .beginControlFlow("if (page != null)")
                .addStatement("return page[indexInPage]")
                .endControlFlow()
                .endControlFlow()
                .addStatement("return $N", defaultValue)
                .build())
            .build();
    }

    private static final Map<TypeName, String> EQUALITY_FUNCTIONS = Map.of(
        TypeName.BYTE, "page[indexInPage] != $N",
        TypeName.SHORT, "page[indexInPage] != $N",
        TypeName.INT, "page[indexInPage] != $N",
        TypeName.LONG, "page[indexInPage] != $N",
        TypeName.FLOAT, "Float.compare(page[indexInPage], $N) != 0",
        TypeName.DOUBLE, "Double.compare(page[indexInPage], $N) != 0"
    );

    private static MethodSpec containsMethod(
        TypeName valueType,
        FieldSpec pages,
        MethodSpec pageIndex,
        MethodSpec indexInPage,
        FieldSpec defaultValue
    ) {
        return MethodSpec.methodBuilder("contains")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .returns(TypeName.BOOLEAN)
            .addCode(CodeBlock.builder()
                .addStatement("int pageIndex = $N(index)", pageIndex)
                .addStatement("int indexInPage = $N(index)", indexInPage)
                .addStatement("$T[] page = $N[pageIndex]", valueType, pages)
                .beginControlFlow("if (page != null)")
                .addStatement("return " + EQUALITY_FUNCTIONS.get(valueType), defaultValue)
                .endControlFlow()
                .addStatement("return false")
                .build())
            .build();
    }

    private static class GrowingBuilderGenerator {

        private static TypeSpec growingBuilder(TypeName elementType, TypeName builderType, TypeName valueType) {
            var builder = TypeSpec.classBuilder("GrowingBuilder")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addSuperinterface(builderType);

            var arrayHandle = arrayHandleField(valueType);
            var pageLock = newPageLockField();
            var defaultValue = defaultValueField(valueType);
            var pages = pagesField(valueType);

            builder.addField(arrayHandle);
            builder.addField(pageLock);
            builder.addField(defaultValue);
            builder.addField(pages);

            builder.addMethod(constructor(valueType, pages, pageLock, defaultValue));

            builder.addMethod(setMethod(valueType));
            builder.addMethod(setIfAbsentMethod(valueType));
            builder.addMethod(addTo(valueType));
            builder.addMethod(buildMethod(elementType));

            return builder.build();
        }

        private static FieldSpec newPageLockField() {
            return FieldSpec
                .builder(ReentrantLock.class, "newPageLock")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build();
        }

        private static FieldSpec arrayHandleField(TypeName valueType) {
            return FieldSpec
                .builder(VarHandle.class, "ARRAY_HANDLE")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer("$T.arrayElementVarHandle($T.class)", MethodHandles.class, ArrayTypeName.of(valueType))
                .build();
        }

        private static FieldSpec pagesField(TypeName valueType) {
            return FieldSpec
                .builder(ParameterizedTypeName.get(
                    ClassName.get(AtomicReferenceArray.class),
                    ArrayTypeName.of(valueType)
                ), "pages")
                .addModifiers(Modifier.PRIVATE)
                .build();
        }

        private static MethodSpec constructor(TypeName valueType, FieldSpec pages, FieldSpec pageLock, FieldSpec defaultValue) {
            return MethodSpec.constructorBuilder()
                .addParameter(valueType, defaultValue.name)
                .addStatement("this.$N = new $T(0)", pages, pages.type)
                .addStatement("this.$N = $N", defaultValue, defaultValue)
                .addStatement("this.$N = new $T(true)", pageLock, pageLock.type)
                .build();
        }

        private static MethodSpec setMethod(TypeName valueType) {
            return MethodSpec.methodBuilder("set")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addParameter(TypeName.LONG, "index")
                .addParameter(valueType, "value")
                .build();
        }

        private static MethodSpec setIfAbsentMethod(TypeName valueType) {
            return MethodSpec.methodBuilder("setIfAbsent")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.BOOLEAN)
                .addParameter(TypeName.LONG, "index")
                .addParameter(valueType, "value")
                .addStatement("return false")
                .build();
        }

        private static MethodSpec addTo(TypeName valueType) {
            return MethodSpec.methodBuilder("addTo")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addParameter(TypeName.LONG, "index")
                .addParameter(valueType, "value")
                .build();
        }

        private static MethodSpec buildMethod(TypeName elementType) {
            return MethodSpec.methodBuilder("build")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(elementType)
                .addStatement("return null")
                .build();
        }
    }
}
