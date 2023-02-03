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
package org.neo4j.gds;

import org.eclipse.collections.impl.block.factory.Functions;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.function.Function;
import java.util.stream.Stream;

public class ProcedureCallContextReturnColumns implements ProcedureReturnColumns {

    private final Stream<String> returnColumns;
    private Function<String, String> transformationFunction;

    public ProcedureCallContextReturnColumns(ProcedureCallContext procedureCallContext) {
        this.returnColumns = procedureCallContext.outputFields();
        this.transformationFunction = Functions.identity();
    }

    @Override
    public boolean contains(String fieldName) {
        return returnColumns.map(transformationFunction).anyMatch(column -> column.equals(fieldName));
    }

    @Override
    public ProcedureReturnColumns withTransformationFunction(Function<String, String> transformationFunction) {
        this.transformationFunction = transformationFunction;
        return this;
    }
}
