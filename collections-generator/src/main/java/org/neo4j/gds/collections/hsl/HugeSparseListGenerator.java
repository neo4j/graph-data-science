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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.collections.CollectionStep;
import org.neo4j.gds.mem.MemoryUsage;

import javax.annotation.processing.Generated;
import javax.lang.model.element.Modifier;
import java.util.Arrays;

import static org.neo4j.gds.collections.EqualityUtils.DEFAULT_VALUES;
import static org.neo4j.gds.collections.EqualityUtils.isEqual;
import static org.neo4j.gds.collections.EqualityUtils.isNotEqual;

final class HugeSparseListGenerator implements CollectionStep.Generator<HugeSparseListValidation.Spec> {

    private static final ClassName ARRAY_UTIL = ClassName.get("org.neo4j.gds.collections", "ArrayUtil");
    private static final ClassName PAGE_UTIL = ClassName.get("org.neo4j.gds.collections", "PageUtil");
    private static final ClassName DRAINING_ITERATOR = ClassName.get("org.neo4j.gds.collections", "DrainingIterator");

    @Override
    public TypeSpec generate(HugeSparseListValidation.Spec spec) {
        var className = ClassName.get(spec.rootPackage().toString(), spec.className());
        var elementType = TypeName.get(spec.element().asType());
        var valueType = TypeName.get(spec.valueType());
        var forAllConsumerType = TypeName.get(spec.forAllConsumerType());

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
        var pageSizeInBytes = pageSizeInBytesField(pageSize);
        builder.addField(pageShift);
        builder.addField(pageSize);
        builder.addField(pageMask);
        builder.addField(pageSizeInBytes);

        // instance fields
        var pages = pagesField(valueType);
        var defaultValue = defaultValueField(valueType);

        builder.addField(pages);
        builder.addField(defaultValue);

        // constructor
        builder.addMethod(constructor(valueType, pages, defaultValue, pageShift));

        // public instance methods
        builder.addMethod(capacityMethod(pages, pageShift));
        builder.addMethod(getMethod(valueType, pages, pageShift, pageMask, defaultValue));
        builder.addMethod(containsMethod(valueType, pages, pageShift, pageMask, defaultValue));
        builder.addMethod(drainingIteratorMethod(valueType, pages, pageSize));
        builder.addMethod(forAllMethod(valueType, forAllConsumerType, pages, pageShift, defaultValue));
        builder.addMethod(setMethod(valueType, pageShift, pageMask));
        if (valueType.isPrimitive()) {
            builder.addMethod(setIfAbsentMethod(valueType, pageShift, pageMask, defaultValue));
            builder.addMethod(addToMethod(valueType, pageShift, pageMask));
        }

        // private instance methods
        builder.addMethod(getPageMethod(valueType, pages));
        builder.addMethod(growMethod(pages));
        builder.addMethod(allocateNewPageMethod(valueType, pages, pageSize, defaultValue));

        return builder.build();
    }

    private static TypeName valueArrayType(TypeName valueType) {
        return ArrayTypeName.of(ArrayTypeName.of(valueType));
    }

    private static AnnotationSpec generatedAnnotation() {
        return AnnotationSpec.builder(Generated.class)
            .addMember("value", "$S", HugeSparseListGenerator.class.getCanonicalName())
            .build();
    }

    private static FieldSpec pageShiftField(int pageShift) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_SHIFT", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("$L", pageShift)
            .build();
    }

    private static FieldSpec pageSizeField(FieldSpec pageShiftField) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_SIZE", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("1 << $N", pageShiftField)
            .build();
    }

    private static FieldSpec pageMaskField(FieldSpec pageSizeField) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_MASK", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("$N - 1", pageSizeField)
            .build();
    }

    private static FieldSpec pageSizeInBytesField(FieldSpec pageSizeField) {
        return FieldSpec
            .builder(TypeName.LONG, "PAGE_SIZE_IN_BYTES", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("$T.sizeOfLongArray($N)", MemoryUsage.class, pageSizeField)
            .build();
    }

    private static FieldSpec pagesField(TypeName valueType) {
        return FieldSpec
            .builder(valueArrayType(valueType), "pages", Modifier.PRIVATE)
            .build();
    }

    private static FieldSpec defaultValueField(TypeName valueType) {
        return FieldSpec
            .builder(valueType, "defaultValue", Modifier.PRIVATE, Modifier.FINAL)
            .build();
    }

    private static MethodSpec constructor(
        TypeName valueType,
        FieldSpec pages,
        FieldSpec defaultValue,
        FieldSpec pageShift
    ) {
        CodeBlock newPagesBlock;

        if (valueType.isPrimitive()) {
            newPagesBlock = CodeBlock.of(
                "this.$N = new $T[numPages][]",
                pages,
                valueType
            );
        } else {
            var componentType = ((ArrayTypeName) valueType).componentType;
            newPagesBlock = CodeBlock.of(
                "this.$N = new $T[numPages][][]",
                pages,
                componentType
            );
        }

        return MethodSpec.constructorBuilder()
            .addParameter(valueType, defaultValue.name)
            .addParameter(TypeName.LONG, "initialCapacity")
            .addStatement("int numPages = $T.pageIndex(initialCapacity, $N)", PAGE_UTIL, pageShift)
            .addStatement(newPagesBlock)
            .addStatement("this.$N = $N", defaultValue, defaultValue)
            .build();
    }

    private static MethodSpec capacityMethod(FieldSpec pages, FieldSpec pageShift) {
        return MethodSpec.methodBuilder("capacity")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.LONG)
            .addStatement("int numPages = $N.length", pages)
            .addStatement("return ((long) numPages) << $N", pageShift)
            .build();
    }

    private static MethodSpec getMethod(
        TypeName valueType,
        FieldSpec pages,
        FieldSpec pageShift,
        FieldSpec pageMask,
        FieldSpec defaultValue
    ) {
        var returnStatementBuilder = CodeBlock.builder();

        if (valueType.isPrimitive()) {
            returnStatementBuilder.addStatement("return page[indexInPage]");
        } else {
            returnStatementBuilder
                .addStatement("$T value = page[indexInPage]", valueType)
                .addStatement("return value == null ? $N : value", defaultValue);
        }

        var returnStatement = returnStatementBuilder.build();

        return MethodSpec.methodBuilder("get")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .returns(valueType)
            .addCode(CodeBlock.builder()
                .addStatement("int pageIndex = $T.pageIndex(index, $N)", PAGE_UTIL, pageShift)
                .addStatement("int indexInPage = $T.indexInPage(index, $N)", PAGE_UTIL, pageMask)
                .beginControlFlow("if (pageIndex < $N.length)", pages)
                .addStatement("$T[] page = $N[pageIndex]", valueType, pages)
                .beginControlFlow("if (page != null)")
                .add(returnStatement)
                .endControlFlow()
                .endControlFlow()
                .addStatement("return $N", defaultValue)
                .build())
            .build();
    }



    private static MethodSpec containsMethod(
        TypeName valueType,
        FieldSpec pages,
        FieldSpec pageShift,
        FieldSpec pageMask,
        FieldSpec defaultValue
    ) {
        return MethodSpec.methodBuilder("contains")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.LONG, "index")
            .returns(TypeName.BOOLEAN)
            .addCode(CodeBlock.builder()
                .addStatement("int pageIndex = $T.pageIndex(index, $N)", PAGE_UTIL, pageShift)
                .beginControlFlow("if (pageIndex < $N.length)", pages)
                .addStatement("$T[] page = $N[pageIndex]", valueType, pages)
                .beginControlFlow("if (page != null)")
                .addStatement("int indexInPage = $T.indexInPage(index, $N)", PAGE_UTIL, pageMask)
                .addStatement("return " + isNotEqual(valueType, "$1L", "$2N", "page[indexInPage]", defaultValue))
                .endControlFlow()
                .endControlFlow()
                .addStatement("return false")
                .build())
            .build();
    }

    private static MethodSpec drainingIteratorMethod(
        TypeName valueType,
        FieldSpec pages,
        FieldSpec pageSize
    ) {
        return MethodSpec.methodBuilder("drainingIterator")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(DRAINING_ITERATOR, ArrayTypeName.of(valueType)))
            .addStatement("return new $T<>($N, $N)", DRAINING_ITERATOR, pages, pageSize)
            .build();
    }

    private static MethodSpec forAllMethod(
        TypeName valueType,
        TypeName forAllConsumerType,
        FieldSpec pages,
        FieldSpec pageShift,
        FieldSpec defaultValue
    ) {
        return MethodSpec.methodBuilder("forAll")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ParameterSpec.builder(forAllConsumerType, "consumer").build())
            .returns(TypeName.VOID)
            .addStatement("$T pages = this.$N", valueArrayType(valueType), pages)
            .beginControlFlow("for (int pageIndex = 0; pageIndex < pages.length; pageIndex++)")
            .addStatement("$T page = pages[pageIndex]", ArrayTypeName.of(valueType))
            .beginControlFlow("if (page == null)")
            .addStatement("continue")
            .endControlFlow() // end if
            .beginControlFlow("for (int indexInPage = 0; indexInPage < page.length; indexInPage++)")
            .addStatement("$T value = page[indexInPage]", valueType)
            .beginControlFlow("if (" + isEqual(valueType, "$1L", "$2N", "value", defaultValue) + ")")
            .addStatement("continue")
            .endControlFlow() // end if
            .addStatement("long index = ((long) pageIndex << $N) | (long) indexInPage", pageShift)
            .addStatement("consumer.consume(index, value)")
            .endControlFlow() // end for
            .endControlFlow() // outer for
            .build();
    }

    private static MethodSpec getPageMethod(TypeName valueType, FieldSpec pages) {
        return MethodSpec.methodBuilder("getPage")
            .addModifiers(Modifier.PRIVATE)
            .returns(ArrayTypeName.of(valueType))
            .addParameter(TypeName.INT, "pageIndex")
            .addCode(CodeBlock.builder()
                .beginControlFlow("if (pageIndex >= $N.length)", pages)
                .addStatement("grow(pageIndex + 1)")
                .endControlFlow()

                .addStatement("$T page = $N[pageIndex]", ArrayTypeName.of(valueType), pages)
                .beginControlFlow("if (page == null)")
                .addStatement("page = allocateNewPage(pageIndex)")
                .endControlFlow()

                .addStatement("return page")

                .build()
            )
            .build();
    }

    private static MethodSpec growMethod(FieldSpec pages) {
        return MethodSpec.methodBuilder("grow")
            .addModifiers(Modifier.PRIVATE)
            .returns(TypeName.VOID)
            .addParameter(TypeName.INT, "minNewSize")
            .addCode(CodeBlock.builder()
                .beginControlFlow("if (minNewSize <= $N.length)", pages)
                .addStatement("return")
                .endControlFlow()
                .addStatement(
                    "int newSize = $T.oversize(minNewSize, $T.BYTES_OBJECT_REF)",
                    ARRAY_UTIL,
                    MemoryUsage.class
                )
                .addStatement("this.$N = $T.copyOf(this.$N, newSize)", pages, Arrays.class, pages)
                .build())
            .build();
    }

    private static MethodSpec allocateNewPageMethod(
        TypeName valueType,
        FieldSpec pages,
        FieldSpec pageSize,
        FieldSpec defaultValue
    ) {
        // 💪
        var bodyBuilder = CodeBlock.builder();

        if (valueType.isPrimitive()) {
            bodyBuilder.addStatement(CodeBlock.of(
                "$T page = new $T[$N]",
                ArrayTypeName.of(valueType),
                valueType,
                pageSize
            ));
        } else {
            var componentType = ((ArrayTypeName) valueType).componentType;
            bodyBuilder.addStatement(CodeBlock.of(
                "$T page = new $T[$N][]",
                ArrayTypeName.of(valueType),
                componentType,
                pageSize
            ));
        }

        // The following is an optimization applicable for primitive
        // types only: If the default value is equal to the default
        // value for the type, there is no need to call Array.fill().
        if (valueType.isPrimitive()) {
            bodyBuilder.beginControlFlow(
                    "if ($L)",
                    isNotEqual(valueType, "$1N", "$2L", defaultValue, DEFAULT_VALUES.get(valueType))
                )
                .addStatement("$T.fill(page, $N)", ClassName.get(Arrays.class), defaultValue)
                .endControlFlow();
        } else {
            bodyBuilder.addStatement("$T.fill(page, $N)", ClassName.get(Arrays.class), defaultValue);
        }


        bodyBuilder
            .addStatement("this.$N[pageIndex] = page", pages)
            .addStatement("return page");

        return MethodSpec.methodBuilder("allocateNewPage")
            .addModifiers(Modifier.PRIVATE)
            .returns(ArrayTypeName.of(valueType))
            .addParameter(TypeName.INT, "pageIndex")
            .addCode(bodyBuilder.build())
            .build();
    }

    private static MethodSpec setMethod(
        TypeName valueType,
        FieldSpec pageShift,
        FieldSpec pageMask
    ) {
        return MethodSpec.methodBuilder("set")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID)
            .addParameter(TypeName.LONG, "index")
            .addParameter(valueType, "value")
            .addCode(CodeBlock.builder()
                .addStatement("int pageIndex = $T.pageIndex(index, $N)", PAGE_UTIL, pageShift)
                .addStatement("int indexInPage = $T.indexInPage(index, $N)", PAGE_UTIL, pageMask)
                .addStatement("getPage(pageIndex)[indexInPage] = value")
                .build()
            )
            .build();
    }

    private static MethodSpec setIfAbsentMethod(
        TypeName valueType,
        FieldSpec pageShift,
        FieldSpec pageMask,
        FieldSpec defaultValue
    ) {
        return MethodSpec.methodBuilder("setIfAbsent")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.BOOLEAN)
            .addParameter(TypeName.LONG, "index")
            .addParameter(valueType, "value")
            .addStatement("int pageIndex = $T.pageIndex(index, $N)", PAGE_UTIL, pageShift)
            .addStatement("int indexInPage = $T.indexInPage(index, $N)", PAGE_UTIL, pageMask)
            .addStatement("$T page = getPage(pageIndex)", ArrayTypeName.of(valueType))
            .addStatement("$T currentValue = page[indexInPage]", valueType)
            .beginControlFlow("if (" + isEqual(valueType, "$1L", "$2N", "currentValue", defaultValue) + ")")
            .addStatement("page[indexInPage] = value")
            .addStatement("return true")
            .endControlFlow()
            .addStatement("return false")
            .build();
    }

    private static MethodSpec addToMethod(
        TypeName valueType,
        FieldSpec pageShift,
        FieldSpec pageMask
    ) {
        return MethodSpec.methodBuilder("addTo")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID)
            .addParameter(TypeName.LONG, "index")
            .addParameter(valueType, "value")
            .addStatement("int pageIndex = $T.pageIndex(index, $N)", PAGE_UTIL, pageShift)
            .addStatement("int indexInPage = $T.indexInPage(index, $N)", PAGE_UTIL, pageMask)
            .addStatement("$T page = getPage(pageIndex)", ArrayTypeName.of(valueType))
            .addStatement("page[indexInPage] += value")
            .build();
    }
}
