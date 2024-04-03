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
package org.neo4j.gds.conductance;

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.community.conductance.ConductanceStreamResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.conductance.Conductance.CONDUCTANCE_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ConductanceStreamProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(value = "gds.conductance.stream", mode = READ)
    @Description(CONDUCTANCE_DESCRIPTION)
    public Stream<ConductanceStreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.community().conductanceStream(graphName, configuration);
    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(value = "gds.alpha.conductance.stream", mode = READ, deprecatedBy = "gds.conductance.stream")
    @Description(CONDUCTANCE_DESCRIPTION)
    public Stream<ConductanceStreamResult> streamAlpha(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.conductance.stream");
        facade
            .log()
            .warn("Procedure `gds.alpha.conductance.stream` has been deprecated, please use `gds.conductance.stream`.");

        return stream(graphName, configuration);
    }
}
