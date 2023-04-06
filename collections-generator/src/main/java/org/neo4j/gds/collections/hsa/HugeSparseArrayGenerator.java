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
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.collections.CollectionStep;
import org.neo4j.gds.mem.MemoryUsage;

import javax.annotation.processing.Generated;
import javax.lang.model.element.Modifier;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.gds.collections.EqualityUtils.DEFAULT_VALUES;
import static org.neo4j.gds.collections.EqualityUtils.isEqual;
import static org.neo4j.gds.collections.EqualityUtils.isNotEqual;

final class HugeSparseArrayGenerator implements CollectionStep.Generator<HugeSparseArrayValidation.Spec> {

    private static final ClassName PAGE_UTIL = ClassName.get("org.neo4j.gds.collections", "PageUtil");
    private static final ClassName ARRAY_UTIL = ClassName.get("org.neo4j.gds.collections", "ArrayUtil");
    private static final ClassName DRAINING_ITERATOR = ClassName.get("org.neo4j.gds.collections", "DrainingIterator");

    @Override
    public TypeSpec generate(HugeSparseArrayValidation.Spec spec) {
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

        // constructor
        builder.addMethod(constructor(valueType));

        // instance methods
        builder.addMethod(capacityMethod(capacity));
        builder.addMethod(getMethod(valueType, pages, pageShift, pageMask, defaultValue));
        builder.addMethod(containsMethod(valueType, pages, pageShift, pageMask, defaultValue));
        builder.addMethod(drainingIteratorMethod(valueType, pages, pageSize));

        // GrowingBuilder
        builder.addType(GrowingBuilderGenerator.growingBuilder(
            className,
            elementType,
            builderType,
            valueType,
            pageSize,
            pageShift,
            pageMask
        ));

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

    private static class GrowingBuilderGenerator {

        private static TypeSpec growingBuilder(
            ClassName className,
            TypeName elementType,
            TypeName builderType,
            TypeName valueType,
            FieldSpec pageSize,
            FieldSpec pageShift,
            FieldSpec pageMask
        ) {
            var builder = TypeSpec.classBuilder("GrowingBuilder")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addSuperinterface(builderType);

            var arrayHandle = arrayHandleField(valueType);
            var pageLock = newPageLockField();
            var defaultValue = defaultValueField(valueType);
            var pages = pagesField(valueType);
            var initialCapacitySpec = initialCapacitySpec();

            builder.addField(arrayHandle);
            builder.addField(pageLock);
            builder.addField(defaultValue);
            builder.addField(pages);

            builder.addMethod(constructor(
                valueType,
                pages,
                pageShift,
                pageLock,
                defaultValue,
                initialCapacitySpec
            ));

            // public methods
            builder.addMethod(setMethod(valueType, pageShift, pageMask, arrayHandle));
            builder.addMethod(buildMethod(valueType, elementType, pages, pageShift, defaultValue, className));

            if (valueType.isPrimitive()) {
                builder.addMethod(setIfAbsentMethod(valueType, pageShift, pageMask, arrayHandle, defaultValue));
                builder.addMethod(addToMethod(valueType, pageShift, pageMask, arrayHandle));
            }

            // helper methods
            builder.addMethod(growMethod(valueType, pageLock, pages));
            builder.addMethod(getPageMethod(valueType, pages));
            builder.addMethod(allocateNewPageMethod(
                valueType,
                pageLock,
                pages,
                pageSize,
                defaultValue
            ));

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

        private static ParameterSpec initialCapacitySpec() {
            return ParameterSpec
                .builder(long.class, "initialCapacity")
                .build();
        }

        private static MethodSpec constructor(
            TypeName valueType,
            FieldSpec pages,
            FieldSpec pageShift,
            FieldSpec pageLock,
            FieldSpec defaultValue,
            ParameterSpec initialCapacity
        ) {
            return MethodSpec.constructorBuilder()
                .addParameter(valueType, defaultValue.name)
                .addParameter(long.class, "initialCapacity")
                .addStatement("int pageCount = $T.pageIndex($N, $N)", PAGE_UTIL, initialCapacity, pageShift)
                .addStatement("this.$N = new $T(pageCount)", pages, pages.type)
                .addStatement("this.$N = $N", defaultValue, defaultValue)
                .addStatement("this.$N = new $T(true)", pageLock, pageLock.type)
                .build();
        }

        private static MethodSpec setMethod(
            TypeName valueType,
            FieldSpec pageShift,
            FieldSpec pageMask,
            FieldSpec arrayHandle
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
                    .addStatement("$N.setVolatile(getPage(pageIndex), indexInPage, value)", arrayHandle)
                    .build()
                )
                .build();
        }

        private static MethodSpec setIfAbsentMethod(
            TypeName valueType,
            FieldSpec pageShift,
            FieldSpec pageMask,
            FieldSpec arrayHandle,
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
                .addStatement(
                    "$T storedValue = ($T) $N.compareAndExchange(getPage(pageIndex), indexInPage, $N, value)",
                    valueType,
                    valueType,
                    arrayHandle,
                    defaultValue
                )
                .addStatement("return " + isEqual(valueType, "$1L", "$2N", "storedValue", defaultValue))
                .build();
        }

        private static MethodSpec addToMethod(
            TypeName valueType,
            FieldSpec pageShift,
            FieldSpec pageMask,
            FieldSpec arrayHandle
        ) {
            return MethodSpec.methodBuilder("addTo")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addParameter(TypeName.LONG, "index")
                .addParameter(valueType, "value")
                .addCode(CodeBlock.builder()
                    .addStatement("int pageIndex = $T.pageIndex(index, $N)", PAGE_UTIL, pageShift)
                    .addStatement("int indexInPage = $T.indexInPage(index, $N)", PAGE_UTIL, pageMask)
                    .addStatement("$T page = getPage(pageIndex)", ArrayTypeName.of(valueType))
                    .addStatement(
                        "$T expectedCurrentValue = ($T) $N.getAcquire(page, indexInPage)",
                        valueType,
                        valueType,
                        arrayHandle
                    )
                    .beginControlFlow("while (true)")
                    .addStatement("$1T newValueToStore = ($1T) (expectedCurrentValue + value)", valueType)
                    .addStatement(
                        "$T actualCurrentValue = ($T) $N.compareAndExchangeRelease(page, indexInPage, expectedCurrentValue, newValueToStore)",
                        valueType,
                        valueType,
                        arrayHandle
                    )
                    .beginControlFlow("if (actualCurrentValue == expectedCurrentValue)")
                    .addStatement("return")
                    .endControlFlow()
                    .addStatement("expectedCurrentValue = actualCurrentValue")
                    .endControlFlow() // eo while
                    .build()
                )
                .build();
        }

        private static MethodSpec buildMethod(
            TypeName valueType,
            TypeName elementType,
            FieldSpec pages,
            FieldSpec pageShift,
            FieldSpec defaultValue,
            ClassName className
        ) {
            final CodeBlock newPagesBlock;
            
            if (valueType.isPrimitive()) {
                newPagesBlock = CodeBlock.of(
                    "$T newPages = new $T[numPages][]",
                    ArrayTypeName.of(ArrayTypeName.of(valueType)),
                    valueType
                );
            } else {
                var componentType = ((ArrayTypeName) valueType).componentType;
                newPagesBlock = CodeBlock.of(
                    "$T newPages = new $T[numPages][][]",
                    ArrayTypeName.of(ArrayTypeName.of(valueType)),
                    componentType
                );
            }

            return MethodSpec.methodBuilder("build")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(elementType)
                .addCode(CodeBlock.builder()
                    .addStatement("int numPages = $N.length()", pages)
                    .addStatement("long capacity = ((long) numPages) << $N", pageShift)
                    .addStatement(newPagesBlock)
                    .addStatement("$T.setAll(newPages, $N::get)", ClassName.get(Arrays.class), pages)
                    .addStatement("return new $T(capacity, newPages, $N)", className, defaultValue)
                    .build()
                ).build();
        }

        private static MethodSpec getPageMethod(TypeName valueType, FieldSpec pages) {
            return MethodSpec.methodBuilder("getPage")
                .addModifiers(Modifier.PRIVATE)
                .returns(ArrayTypeName.of(valueType))
                .addParameter(TypeName.INT, "pageIndex")
                .addCode(CodeBlock.builder()
                    .beginControlFlow("if (pageIndex >= $N.length())", pages)
                    .addStatement("grow(pageIndex + 1)")
                    .endControlFlow()

                    .addStatement("$T page = $N.get(pageIndex)", ArrayTypeName.of(valueType), pages)
                    .beginControlFlow("if (page == null)")
                    .addStatement("page = allocateNewPage(pageIndex)")
                    .endControlFlow()

                    .addStatement("return page")

                    .build()
                )
                .build();
        }

        private static MethodSpec growMethod(
            TypeName valueType,
            FieldSpec pageLock,
            FieldSpec pages
        ) {
            return MethodSpec.methodBuilder("grow")
                .addModifiers(Modifier.PRIVATE)
                .returns(TypeName.VOID)
                .addParameter(TypeName.INT, "newSize")
                .addCode(CodeBlock.builder()
                    .addStatement("$N.lock()", pageLock)
                    .beginControlFlow("try")
                    .beginControlFlow("if (newSize <= $N.length())", pages)
                    .addStatement("return")
                    .endControlFlow() // eo if (newSize <= pages.length())
                    // TODO avoid using FQN literal for HugeArrays (e.g. by introducing collections-util module)
                    .addStatement(
                        "$T newPages = new $T($T.oversize(newSize, $T.BYTES_OBJECT_REF))",
                        pages.type,
                        pages.type,
                        ARRAY_UTIL,
                        MemoryUsage.class
                    )
                    .beginControlFlow("for (int pageIndex = 0; pageIndex < $N.length(); pageIndex++)", pages)
                    .addStatement("$T page = $N.get(pageIndex)", ArrayTypeName.of(valueType), pages)
                    .beginControlFlow("if (page != null)")
                    .addStatement("newPages.set(pageIndex, page)")
                    .endControlFlow() // eo if (page != null)
                    .endControlFlow() // eo for
                    .addStatement("$N = newPages", pages)
                    .endControlFlow() // eo try
                    .beginControlFlow("finally")
                    .addStatement("$N.unlock()", pageLock)
                    .endControlFlow() // eo finally
                    .build())
                .build();
        }

        private static MethodSpec allocateNewPageMethod(
            TypeName valueType,
            FieldSpec pageLock,
            FieldSpec pages,
            FieldSpec pageSize,
            FieldSpec defaultValue
        ) {
            final CodeBlock pageAssignmentBlock;

            if (valueType.isPrimitive()) {
                pageAssignmentBlock = CodeBlock.of("page = new $T[$N]", valueType, pageSize);
            } else {
                var componentType = ((ArrayTypeName) valueType).componentType;
                pageAssignmentBlock = CodeBlock.of("page = new $T[$N][]", componentType, pageSize);
            }

            // 💪
            var bodyBuilder = CodeBlock.builder()
                .addStatement("$N.lock()", pageLock)
                .beginControlFlow("try")
                .addStatement("$T page = $N.get(pageIndex)", ArrayTypeName.of(valueType), pages)
                .beginControlFlow("if (page != null)")
                .addStatement("return page")
                .endControlFlow()
                .addStatement(pageAssignmentBlock);

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
            }

            bodyBuilder.addStatement("$N.set(pageIndex, page)", pages)
                .addStatement("return page")
                .nextControlFlow("finally")
                .addStatement("$N.unlock()", pageLock)
                .endControlFlow();  // eo try/finally

            return MethodSpec.methodBuilder("allocateNewPage")
                .addModifiers(Modifier.PRIVATE)
                .returns(ArrayTypeName.of(valueType))
                .addParameter(TypeName.INT, "pageIndex")
                .addCode(bodyBuilder.build())
                .build();
        }
    }
}
