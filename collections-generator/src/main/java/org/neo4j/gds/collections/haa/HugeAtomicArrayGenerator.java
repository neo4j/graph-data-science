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
import org.neo4j.gds.mem.MemoryUsage;

import javax.annotation.processing.Generated;
import javax.lang.model.element.Modifier;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

final class HugeAtomicArrayGenerator implements CollectionStep.Generator<HugeAtomicArrayValidation.Spec> {

    static final ClassName PAGE_UTIL = ClassName.get("org.neo4j.gds.collections", "PageUtil");

    static final String DEFAULT_VALUE_METHOD = "defaultValue";

    @Override
    public TypeSpec generate(HugeAtomicArrayValidation.Spec spec) {
        var className = ClassName.get(spec.rootPackage().toString(), spec.className());
        var elementType = TypeName.get(spec.element().asType());
        var valueType = TypeName.get(spec.valueType());
        var unaryOperatorType = TypeName.get(spec.valueOperatorInterface());
        var pageCreatorType = TypeName.get(spec.pageCreatorInterface());

        var builder = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.ABSTRACT)
            .addSuperinterface(elementType)
            .addOriginatingElement(spec.element());

        // class annotation
        builder.addAnnotation(generatedAnnotation());

        // static methods
        builder.addMethod(memoryEstimationMethod());

        // constructor
        builder.addMethod(ofMethod(elementType, pageCreatorType));

        builder.addType(SingleArrayBuilder.builder(
            elementType,
            className,
            valueType,
            unaryOperatorType,
            pageCreatorType
        ));

        builder.addType(PagedArrayBuilder.builder(
            elementType,
            className,
            valueType,
            unaryOperatorType,
            pageCreatorType,
            spec.pageShift()
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

    static FieldSpec arrayHandleField(TypeName valueType) {
        return FieldSpec
            .builder(VarHandle.class, "ARRAY_HANDLE", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer("$T.arrayElementVarHandle($T.class)", MethodHandles.class, valueArrayType(valueType))
            .build();
    }

    private static MethodSpec ofMethod(TypeName interfaceType, TypeName pageCreatorType) {
        return MethodSpec.methodBuilder("of")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(TypeName.LONG, "size")
            .addParameter(pageCreatorType, "pageCreator")
            .returns(interfaceType)
            .beginControlFlow("if (size <= $T.MAX_ARRAY_LENGTH)", PAGE_UTIL)
            .addStatement("return $N.of(size, pageCreator)", SingleArrayBuilder.SINLGE_CLASS_NAME)
            .endControlFlow()
            .addStatement("return $N.of(size, pageCreator)", PagedArrayBuilder.PAGED_CLASS_NAME)
            .build();
    }

    private static MethodSpec memoryEstimationMethod() {
        return MethodSpec.methodBuilder("memoryEstimation")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(TypeName.LONG, "size")
            .returns(TypeName.LONG)
            .addStatement("assert size >= 0")
            .beginControlFlow("if (size <= $T.MAX_ARRAY_LENGTH)", PAGE_UTIL)
            .addStatement(
                "return $1T.sizeOfInstance($2N.class) + $2N.memoryEstimation(size)",
                MemoryUsage.class,
                SingleArrayBuilder.SINLGE_CLASS_NAME
            )
            .endControlFlow()
            .addStatement(
                "return $1T.sizeOfInstance($2N.class) + $2N.memoryEstimation(size)",
                MemoryUsage.class,
                PagedArrayBuilder.PAGED_CLASS_NAME
            )
            .build();
    }
}
