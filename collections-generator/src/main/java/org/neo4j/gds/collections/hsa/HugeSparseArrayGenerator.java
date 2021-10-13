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
import org.neo4j.gds.mem.MemoryUsage;

import javax.annotation.processing.Generated;
import javax.lang.model.element.Modifier;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

import static java.util.Map.entry;

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
        var pageSizeInBytes = pageSizeInBytesField(pageSize);
        builder.addField(pageShift);
        builder.addField(pageSize);
        builder.addField(pageMask);
        builder.addField(pageSizeInBytes);

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
        builder.addType(GrowingBuilderGenerator.growingBuilder(
            className,
            elementType,
            builderType,
            valueType,
            pageSize,
            pageShift,
            pageIndex,
            indexInPage,
            pageSizeInBytes
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

    private static FieldSpec pageSizeInBytesField(FieldSpec pageSizeField) {
        return FieldSpec
            .builder(TypeName.LONG, "PAGE_SIZE_IN_BYTES", Modifier.STATIC, Modifier.PRIVATE, Modifier.FINAL)
            .initializer("$T.sizeOfLongArray($N)", MemoryUsage.class, pageSizeField)
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
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .addParameter(TypeName.LONG, "index")
            .returns(TypeName.INT)
            .addStatement("return (int) (index >>> $N)", pageShift)
            .build();
    }

    private static MethodSpec indexInPageMethod(FieldSpec pageMask) {
        return MethodSpec.methodBuilder("indexInPage")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
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
                .addStatement("int pageIndex = $N(index)", pageIndex)
                .addStatement("int indexInPage = $N(index)", indexInPage)
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

    private static final Map<TypeName, String> EQUAL_PREDICATES = Map.ofEntries(
        entry(TypeName.BYTE, "%s == %s"),
        entry(TypeName.SHORT, "%s == %s"),
        entry(TypeName.INT, "%s == %s"),
        entry(TypeName.LONG, "%s == %s"),
        entry(TypeName.FLOAT, "Float.compare(%s, %s) == 0"),
        entry(TypeName.DOUBLE, "Double.compare(%s, %s) == 0"),
        entry(ArrayTypeName.of(TypeName.BYTE), "Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.SHORT), "Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.INT), "Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.LONG), "Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.FLOAT), "Arrays.equals(%1$s, %2$s)"),
        entry(ArrayTypeName.of(TypeName.DOUBLE), "Arrays.equals(%1$s, %2$s)")
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
        entry(ArrayTypeName.of(TypeName.DOUBLE), "%1$s != null && !Arrays.equals(%1$s, %2$s)")
    );

    private static <LHS, RHS> CodeBlock isEqual(
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

    private static <LHS, RHS> CodeBlock isNotEqual(
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

    private static final Map<TypeName, String> DEFAULT_VALUES = Map.of(
        TypeName.BYTE, Byte.toString((new byte[1])[0]),
        TypeName.SHORT, Short.toString((new short[1])[0]),
        TypeName.INT, Integer.toString((new int[1])[0]),
        TypeName.LONG, Long.toString((new long[1])[0]),
        TypeName.FLOAT, Float.toString((new float[1])[0]),
        TypeName.DOUBLE, Double.toString((new double[1])[0])
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
                .addStatement("return " + isNotEqual(valueType, "$1L", "$2N", "page[indexInPage]", defaultValue))
                .endControlFlow()
                .addStatement("return false")
                .build())
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
            MethodSpec pageIndex,
            MethodSpec indexInPage,
            FieldSpec pageSizeInBytes
        ) {
            var builder = TypeSpec.classBuilder("GrowingBuilder")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addSuperinterface(builderType);

            var arrayHandle = arrayHandleField(valueType);
            var pageLock = newPageLockField();
            var defaultValue = defaultValueField(valueType);
            var pages = pagesField(valueType);
            var trackAllocation = trackAllocationField();

            builder.addField(arrayHandle);
            builder.addField(pageLock);
            builder.addField(defaultValue);
            builder.addField(pages);
            builder.addField(trackAllocation);

            builder.addMethod(constructor(valueType, pages, pageLock, defaultValue, trackAllocation));

            // public methods
            builder.addMethod(setMethod(valueType, pageIndex, indexInPage, arrayHandle));
            builder.addMethod(buildMethod(valueType, elementType, pages, pageShift, defaultValue, className));

            if (valueType.isPrimitive()) {
                builder.addMethod(setIfAbsentMethod(valueType, pageIndex, indexInPage, arrayHandle, defaultValue));
                builder.addMethod(addToMethod(valueType, pageIndex, indexInPage, arrayHandle));
            }

            // helper methods
            builder.addMethod(growMethod(valueType, pageLock, pages));
            builder.addMethod(getPageMethod(valueType, pages));
            builder.addMethod(allocateNewPageMethod(
                valueType,
                pageLock,
                pages,
                pageSize,
                defaultValue,
                trackAllocation,
                pageSizeInBytes
            ));

            return builder.build();
        }

        private static FieldSpec newPageLockField() {
            return FieldSpec
                .builder(ReentrantLock.class, "newPageLock")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build();
        }

        private static FieldSpec trackAllocationField() {
            return FieldSpec
                .builder(LongConsumer.class, "trackAllocation")
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

        private static MethodSpec constructor(
            TypeName valueType,
            FieldSpec pages,
            FieldSpec pageLock,
            FieldSpec defaultValue,
            FieldSpec trackAllocation
        ) {
            return MethodSpec.constructorBuilder()
                .addParameter(valueType, defaultValue.name)
                .addParameter(LongConsumer.class, "trackAllocation")
                .addStatement("this.$N = new $T(0)", pages, pages.type)
                .addStatement("this.$N = $N", defaultValue, defaultValue)
                .addStatement("this.$N = new $T(true)", pageLock, pageLock.type)
                .addStatement("this.$N = trackAllocation", trackAllocation)
                .build();
        }

        private static MethodSpec setMethod(
            TypeName valueType,
            MethodSpec pageIndex,
            MethodSpec indexInPage,
            FieldSpec arrayHandle
        ) {
            return MethodSpec.methodBuilder("set")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addParameter(TypeName.LONG, "index")
                .addParameter(valueType, "value")
                .addCode(CodeBlock.builder()
                    .addStatement("int pageIndex = $N(index)", pageIndex)
                    .addStatement("int indexInPage = $N(index)", indexInPage)
                    .addStatement("$N.setVolatile(getPage(pageIndex), indexInPage, value)", arrayHandle)
                    .build()
                )
                .build();
        }

        private static MethodSpec setIfAbsentMethod(
            TypeName valueType,
            MethodSpec pageIndex,
            MethodSpec indexInPage,
            FieldSpec arrayHandle,
            FieldSpec defaultValue
        ) {
            return MethodSpec.methodBuilder("setIfAbsent")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.BOOLEAN)
                .addParameter(TypeName.LONG, "index")
                .addParameter(valueType, "value")
                .addStatement("int pageIndex = $N(index)", pageIndex)
                .addStatement("int indexInPage = $N(index)", indexInPage)
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
            MethodSpec pageIndex,
            MethodSpec indexInPage,
            FieldSpec arrayHandle
        ) {
            return MethodSpec.methodBuilder("addTo")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addParameter(TypeName.LONG, "index")
                .addParameter(valueType, "value")
                .addCode(CodeBlock.builder()
                    .addStatement("int pageIndex = $N(index)", pageIndex)
                    .addStatement("int indexInPage = $N(index)", indexInPage)
                    .addStatement("$T page = getPage(pageIndex)", ArrayTypeName.of(valueType))
                    .addStatement(
                        "$T expectedCurrentValue = ($T) $N.getVolatile(page, indexInPage)",
                        valueType,
                        valueType,
                        arrayHandle
                    )
                    .beginControlFlow("while (true)")
                    .addStatement("var newValueToStore = expectedCurrentValue + value")
                    .addStatement(
                        "$T actualCurrentValue = ($T) $N.compareAndExchange(page, indexInPage, expectedCurrentValue, newValueToStore)",
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
                        "$T newPages = new $T(org.neo4j.gds.mem.HugeArrays.oversizeInt(newSize, $T.BYTES_OBJECT_REF))",
                        pages.type,
                        pages.type,
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
            FieldSpec defaultValue,
            FieldSpec trackAllocation,
            FieldSpec pageSizeInBytes
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
                .addStatement("$N.accept($N)", trackAllocation, pageSizeInBytes)
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
