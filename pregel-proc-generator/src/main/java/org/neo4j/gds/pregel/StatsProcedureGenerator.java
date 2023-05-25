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
package org.neo4j.gds.pregel;

import com.squareup.javapoet.AnnotationSpec;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.pregel.generator.TypeNames;
import org.neo4j.gds.pregel.proc.PregelStatsProc;
import org.neo4j.gds.pregel.proc.PregelStatsResult;

import java.util.Optional;

class StatsProcedureGenerator extends WriteProcedureGenerator {

    StatsProcedureGenerator(
        Optional<AnnotationSpec> generatedAnnotationSpec,
        TypeNames typeNames,
        String procedureName,
        Optional<String> description,
        boolean requiresInverseIndex
    ) {
        super(generatedAnnotationSpec, typeNames, procedureName, description, requiresInverseIndex);
    }

    @Override
    GDSMode procGdsMode() {
        return GDSMode.STATS;
    }

    @Override
    org.neo4j.procedure.Mode procExecMode() {
        return org.neo4j.procedure.Mode.READ;
    }

    @Override
    Class<?> procBaseClass() {
        return PregelStatsProc.class;
    }

    @Override
    Class<?> procResultClass() {
        return PregelStatsResult.class;
    }

    @Override
    Class<?> procResultBuilderClass() {
        return PregelStatsResult.Builder.class;
    }
}
