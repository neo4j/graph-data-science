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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.collections.CollectionStep;

import javax.annotation.processing.Generated;
import javax.lang.model.element.Modifier;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.stream.IntStream;

final class HugeAtomicArrayGenerator implements CollectionStep.Generator<HugeAtomicArrayValidation.Spec> {

    static final ClassName PAGE_UTIL = ClassName.get("org.neo4j.gds.collections", "PageUtil");

    @Override
    public TypeSpec generate(HugeAtomicArrayValidation.Spec spec) {
        var className = ClassName.get(spec.rootPackage().toString(), spec.className());
        var elementType = TypeName.get(spec.element().asType());
        var valueType = TypeName.get(spec.valueType());
        var unaryOperatorType = TypeName.get(spec.valueOperatorInterface());

        var builder = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.ABSTRACT)
            .addSuperinterface(elementType)
            .addOriginatingElement(spec.element());

        // class annotation
        builder.addAnnotation(generatedAnnotation());

        // TODO add single

        // class fields
        var arrayHandle = arrayHandleField(valueType);
        var pageShift = pageShiftField(spec.pageShift());
        var pageSize = pageSizeField(pageShift);
        var pageMask = pageMaskField(pageSize);
        builder.addField(arrayHandle);
        builder.addField(pageShift);
        builder.addField(pageSize);
        builder.addField(pageMask);

        // static methods
        builder.addMethod(memoryEstimationMethod());

        // constructor
        // TODO idx to value producer as a parameter
        builder.addMethod(ofMethod(valueType, elementType, pageMask, pageShift, pageSize));

        builder.addType(PageArrayBuilder.builder(
            elementType,
            className,
            valueType,
            unaryOperatorType,
            arrayHandle,
            pageShift,
            pageSize,
            pageMask
        ));

        return builder.build();
    }

    static TypeName valueArrayType(TypeName valueType) {
        return ArrayTypeName.of(valueType);
    }

    private static AnnotationSpec generatedAnnotation() {
        return AnnotationSpec.builder(Generated.class)
            .addMember("value", "$S", HugeAtomicArrayGenerator.class.getCanonicalName())
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
            .builder(TypeName.INT, "PAGE_SIZE", Modifier.STATIC, Modifier.FINAL)
            .initializer("1 << $N", pageShiftField)
            .build();
    }

    private static FieldSpec pageMaskField(FieldSpec pageSizeField) {
        return FieldSpec
            .builder(TypeName.INT, "PAGE_MASK", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("$N - 1", pageSizeField)
            .build();
    }

    private static FieldSpec arrayHandleField(TypeName valueType) {
        return FieldSpec
            .builder(VarHandle.class, "ARRAY_HANDLE", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer("$T.arrayElementVarHandle($T.class)", MethodHandles.class, valueArrayType(valueType))
            .build();
    }

    private static MethodSpec ofMethod(
        TypeName valueType,
        TypeName interfaceType,
        FieldSpec pageMask,
        FieldSpec pageShift,
        FieldSpec pageSize
    ) {
        return MethodSpec.methodBuilder("of")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(TypeName.LONG, "size")
            .returns(interfaceType)
            .addStatement("int numPages = $T.numPagesFor(size, $N, $N)", PAGE_UTIL, pageShift, pageMask)
            .addStatement("$T pages = new $T[numPages][]", valueArrayType(valueArrayType(valueType)), valueType)
            .addStatement("int lastPageSize = $T.exclusiveIndexOfPage(size, $N)", PAGE_UTIL, pageMask)
            .addStatement("int lastPageIndex = pages.length - 1")
            .addStatement("$T.range(0, lastPageIndex).forEach(idx -> pages[idx] = new $T[$N])", IntStream.class, valueType, pageSize)
            .addStatement("pages[lastPageIndex] = new $T[lastPageSize]", valueType)
            .addStatement("long memoryUsed = memoryEstimation(size)")
            .addStatement("return new $N(size, pages, memoryUsed)", PageArrayBuilder.PAGED_CLASS_NAME)
            .build();
    }

    private static MethodSpec memoryEstimationMethod() {
        return MethodSpec.methodBuilder("memoryEstimation")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(TypeName.LONG, "size")
            .returns(TypeName.LONG)
            .addStatement("assert size >= 0")
            .addStatement("return $N.memoryEstimation(size)", PageArrayBuilder.PAGED_CLASS_NAME)
            .build();
    }
}
