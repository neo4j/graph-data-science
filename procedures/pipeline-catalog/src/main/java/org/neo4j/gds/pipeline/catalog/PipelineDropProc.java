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
package org.neo4j.gds.pipeline.catalog;

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.pipelines.PipelineCatalogResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class PipelineDropProc {
    private static final String DESCRIPTION = "Drops a pipeline and frees up the resources it occupies.";

    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.pipeline.drop", mode = READ)
    @Description(DESCRIPTION)
    public Stream<PipelineCatalogResult> drop(
        @Name(value = "pipelineName") String pipelineName,
        @Name(value = "failIfMissing", defaultValue = "true") boolean failIfMissing
    ) {
        return facade.pipelines().drop(pipelineName, failIfMissing);
    }

    @Procedure(name = "gds.beta.pipeline.drop", mode = READ, deprecatedBy = "gds.pipeline.drop")
    @Description(DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<PipelineCatalogResult> betaDrop(
        @Name(value = "pipelineName") String pipelineName,
        @Name(value = "failIfMissing", defaultValue = "true") boolean failIfMissing
    ) {
        facade.deprecatedProcedures().called("gds.beta.pipeline.drop");
        facade
            .log()
            .warn("Procedure `gds.beta.pipeline.drop` has been deprecated, please use `gds.pipeline.drop`.");

        return drop(pipelineName, failIfMissing);
    }
}
