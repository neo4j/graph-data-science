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
package org.neo4j.gds.beta.pregel;

import com.squareup.javapoet.MethodSpec;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.pregel.proc.PregelStreamProc;
import org.neo4j.gds.pregel.proc.PregelStreamResult;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;

import static org.neo4j.gds.beta.pregel.annotation.GDSMode.STREAM;

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
        return PregelStreamProc.class;
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
}
