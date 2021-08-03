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

import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;

import javax.lang.model.SourceVersion;
import javax.lang.model.util.Elements;

class MutateProcedureGenerator extends WriteProcedureGenerator {

    MutateProcedureGenerator(
        Elements elementUtils,
        SourceVersion sourceVersion,
        PregelValidation.Spec pregelSpec
    ) {
        super(elementUtils, sourceVersion, pregelSpec);
    }

    @Override
    GDSMode procGdsMode() {
        return GDSMode.MUTATE;
    }

    @Override
    org.neo4j.procedure.Mode procExecMode() {
        return org.neo4j.procedure.Mode.READ;
    }

    @Override
    Class<?> procBaseClass() {
        return PregelMutateProc.class;
    }

    @Override
    Class<?> procResultClass() {
        return PregelMutateResult.class;
    }

    @Override
    Class<?> procResultBuilderClass() {
        return PregelMutateResult.Builder.class;
    }
}
