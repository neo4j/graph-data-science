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
package org.neo4j.gds.encoding;

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;

import static org.neo4j.gds.encoding.Constants.DESCRIPTION;

public class OneHotEncodingFunction {
    @Context
    public GraphDataScienceProcedures facade;

    @UserFunction("gds.util.oneHotEncoding")
    @Description(DESCRIPTION)
    public List<Long> oneHotEncoding(
        @Name(value = "availableValues") List<Object> availableValues,
        @Name(value = "selectedValues") List<Object> selectedValues
    ) {
        return facade.functions().oneHotEncoding(availableValues, selectedValues);
    }

    @UserFunction(value = "gds.alpha.ml.oneHotEncoding", deprecatedBy = "gds.util.oneHotEncoding")
    @Description(DESCRIPTION)
    @Internal
    @Deprecated
    public List<Long> alphaOneHotEncoding(
        @Name(value = "availableValues") List<Object> availableValues,
        @Name(value = "selectedValues") List<Object> selectedValues
    ) {
        facade.deprecatedProcedures().called("gds.alpha.ml.oneHotEncoding");
        facade.log().warn(
            "Function `gds.alpha.ml.oneHotEncoding` has been deprecated, please use `gds.util.oneHotEncoding`.");

        return oneHotEncoding(availableValues, selectedValues);
    }
}
