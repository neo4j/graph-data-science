/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.pregel;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;

class AlgorithmGenerator extends PregelGenerator {
    AlgorithmGenerator(Elements elementUtils, SourceVersion sourceVersion) {
        super(elementUtils, sourceVersion);
    }

    TypeSpec typeSpec(PregelValidation.Spec pregelSpec) {
        TypeName configTypeName = configTypeName(pregelSpec);
        ClassName algorithmClassName = ClassName.get(
            pregelSpec.rootPackage(),
            pregelSpec.computationName() + "Algorithm"
        );

        var typeSpecBuilder = TypeSpec
            .classBuilder(algorithmClassName)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(Algorithm.class),
                algorithmClassName,
                ClassName.get(HugeDoubleArray.class)
            ))
            .addOriginatingElement(pregelSpec.element());

        addGeneratedAnnotation(typeSpecBuilder);

        typeSpecBuilder.addMethod(computeMethod());
        typeSpecBuilder.addMethod(meMethod(algorithmClassName));
        typeSpecBuilder.addMethod(releaseMethod());

        return typeSpecBuilder.build();
    }

    private MethodSpec computeMethod() {
        return MethodSpec.methodBuilder("compute")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(HugeDoubleArray.class)
            // TODO return pregelJob.run(maxIterations);
            .addStatement("return null")
            .build();
    }

    private MethodSpec releaseMethod() {
        return MethodSpec.methodBuilder("release")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .build();
    }

    private MethodSpec meMethod(ClassName algorithmClassName) {
        return MethodSpec.methodBuilder("me")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(algorithmClassName)
            .addStatement("return this")
            .build();
    }
}
