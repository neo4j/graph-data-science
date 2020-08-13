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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.beta.pregel.annotation.GDSMode.STREAM;

class StreamProcedureGenerator extends ProcedureGenerator {

    StreamProcedureGenerator(Elements elementUtils, SourceVersion sourceVersion, PregelValidation.Spec pregelSpec) {
        super(elementUtils, sourceVersion, pregelSpec);
    }

    @Override
    GDSMode procGdsMode() {
        return STREAM;
    }

    @Override
    org.neo4j.procedure.Mode procExecMode() {
        return org.neo4j.procedure.Mode.READ;
    }

    @Override
    Class<?> procBaseClass() {
        return StreamProc.class;
    }

    @Override
    Class<?> procResultClass() {
        return PregelStreamResult.class;
    }

    @Override
    MethodSpec procResultMethod() {
        return MethodSpec.methodBuilder("streamResult")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(procResultClass())
            .addParameter(long.class, "originalNodeId")
            .addParameter(long.class, "internalNodeId")
            .addParameter(NodeProperties.class, "nodeProperties")
            .addStatement("throw new $T()", UnsupportedOperationException.class)
            .build();
    }

    @Override
    protected List<MethodSpec> additionalMethods() {
        return List.of(streamMethod());
    }

    private MethodSpec streamMethod() {
        var streamResultBlock = CodeBlock.builder()
            .add("$T values = result.schema().entrySet().stream()", ParameterizedTypeName.get(Map.class, String.class, Object.class))
            .add(".collect(")
            .add(CodeBlock.builder()
                .add("$T.toMap(\n", Collectors.class)
                .add("$T::getKey,\n", Map.Entry.class)
                .beginControlFlow("entry ->")
                .add(CodeBlock.builder()
                    .beginControlFlow("if (entry.getValue() == $T.DOUBLE)", ValueType.class)
                    .addStatement("return result.doubleProperties(entry.getKey()).get(nodeId)")
                    .endControlFlow()
                    .addStatement("return result.longProperties(entry.getKey()).get(nodeId)")
                    .build()
                )
                .endControlFlow()
                .add(")")
                .build()
            )
            .add(");")
            .add("\n")
            .addStatement("return new $T(nodeId, values)", PregelStreamResult.class)
            .build();

        return MethodSpec.methodBuilder("stream")
            .addModifiers(Modifier.PROTECTED)
            .addAnnotation(Override.class)
            .returns(ParameterizedTypeName.get(Stream.class, PregelStreamResult.class))
            .addParameter(ParameterizedTypeName.get(
                ClassName.get(AlgoBaseProc.ComputationResult.class),
                className(pregelSpec, ALGORITHM_SUFFIX),
                ClassName.get(Pregel.PregelResult.class),
                pregelSpec.configTypeName()
            ), "computationResult")
            .beginControlFlow("if (computationResult.isGraphEmpty())")
            .addStatement("return $T.empty()", Stream.class)
            .endControlFlow()
            .addStatement("var result = computationResult.result().compositeNodeValues()")
            .addCode(CodeBlock.builder()
                .add("return $T", LongStream.class)
                .add(".range($T.START_NODE_ID, computationResult.graph().nodeCount())", IdMapping.class)
                .beginControlFlow(".mapToObj(nodeId ->")
                .add(streamResultBlock)
                .endControlFlow(")")
                .build()
            )
            .addCode("\n")
            .build();
    }
}
