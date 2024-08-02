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

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.operations.ProgressResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class ListProgressProc {
    private static final String DESCRIPTION = "List progress events for currently running tasks.";

    @Context
    public GraphDataScienceProcedures facade;

    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(value = "gds.beta.listProgress", deprecatedBy = "gds.listProgress")
    @Description(DESCRIPTION)
    public Stream<ProgressResult> betaListProgress(@Name(value = "jobId", defaultValue = "") String jobId) {
        facade.deprecatedProcedures().called("gds.beta.listProgress");
        facade.log().warn("Procedure `gds.beta.listProgress` has been deprecated, please use `gds.listProgress`.");

        return listProgress(jobId);
    }

    @Procedure("gds.listProgress")
    @Description(DESCRIPTION)
    public Stream<ProgressResult> listProgress(@Name(value = "jobId", defaultValue = "") String jobId) {
        return facade.operations().listProgress(jobId);
    }
}
