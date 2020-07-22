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
package org.neo4j.graphalgo.beta.pregel;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;
import java.util.Map;
import java.util.stream.Stream;

class WriteProcedureGenerator extends ProcedureGenerator {

    WriteProcedureGenerator(Elements elementUtils, SourceVersion sourceVersion) {
        super(elementUtils, sourceVersion);
    }

    @Override
    String procClassInfix() {
        return "Write";
    }

    @Override
    Class<?> procBaseClass() {
        return WriteProc.class;
    }

    @Override
    Class<?> procResultClass() {
        return PregelWriteResult.class;
    }

    @Override
    MethodSpec procMethod(PregelValidation.Spec pregelSpec) {
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("write");

        // add procedure annotation
        methodBuilder.addAnnotation(AnnotationSpec.builder(org.neo4j.procedure.Procedure.class)
            .addMember("name", "$S", pregelSpec.procedureName() + "." + procClassInfix().toLowerCase())
            .addMember("mode", "$T.WRITE", Mode.class)
            .build()
        );
        // add description
        pregelSpec.description().ifPresent(annotationMirror -> methodBuilder.addAnnotation(AnnotationSpec.get(annotationMirror)));

        return methodBuilder
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ParameterSpec.builder(Object.class, "graphNameOrConfig")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "graphName")
                    .build())
                .build())
            .addParameter(ParameterSpec
                .builder(ParameterizedTypeName.get(Map.class, String.class, Object.class), "configuration")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "configuration")
                    .addMember("defaultValue", "$S", "{}")
                    .build())
                .build())
            .addStatement("return write(compute(graphNameOrConfig, configuration))")
            .returns(ParameterizedTypeName.get(Stream.class, PregelWriteResult.class))
            .build();
    }

    @Override
    MethodSpec procResultMethod(PregelValidation.Spec pregelSpec) {
        return MethodSpec.methodBuilder("resultBuilder")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(ParameterizedTypeName.get(AbstractResultBuilder.class, PregelWriteResult.class))
            .addParameter(ParameterizedTypeName.get(
                ClassName.get(AlgoBaseProc.ComputationResult.class),
                className(pregelSpec, ALGORITHM_SUFFIX),
                ClassName.get(HugeDoubleArray.class),
                pregelSpec.configTypeName()
            ), "computeResult")
            .addStatement("return new $T()", PregelWriteResult.Builder.class)
            .build();
    }
}
