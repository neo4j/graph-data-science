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
package org.neo4j.gds.harmonic;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityWriteResult;
import org.neo4j.gds.procedures.centrality.alphaharmonic.AlphaHarmonicWriteResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.harmonic.HarmonicCentralityCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

public class HarmonicCentralityWriteProc extends BaseProc {
    
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(value = "gds.closeness.harmonic.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<CentralityWriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.centrality().harmonicCentralityWrite(graphName, configuration);
    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(value = "gds.alpha.closeness.harmonic.write", mode = WRITE, deprecatedBy = "gds.closeness.harmonic.write")
    @Description(DESCRIPTION)
    public Stream<AlphaHarmonicWriteResult> alphaWrite(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade
            .deprecatedProcedures().called("gds.alpha.closeness.harmonic.write");

        facade
            .log()
            .warn("Procedure `gds.alpha.closeness.harmonic.write` has been deprecated, please use `gds.closeness.harmonic.write`.");

        return facade.centrality().alphaHarmonicCentralityWrite(graphName, configuration);

    }


}
