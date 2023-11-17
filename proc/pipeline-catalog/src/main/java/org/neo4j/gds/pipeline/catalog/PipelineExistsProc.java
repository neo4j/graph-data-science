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

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class PipelineExistsProc extends BaseProc {

    private static final String DESCRIPTION = "Checks if a given pipeline exists in the pipeline catalog.";

    @Procedure(name = "gds.pipeline.exists", mode = READ)
    @Description(DESCRIPTION)
    public Stream<PipelineExistsResult> exists(@Name(value = "pipelineName") String pipelineName) {
        CypherMapAccess.failOnBlank("pipelineName", pipelineName);

        String type;
        boolean exists;

        if (PipelineCatalog.exists(username(), pipelineName)) {
            exists = true;
            type = PipelineCatalog.get(username(), pipelineName).type();
        } else {
            exists = false;
            type = "n/a";
        }
        return Stream.of(new PipelineExistsResult(
            pipelineName,
            type,
            exists
        ));
    }

    @Procedure(name = "gds.beta.pipeline.exists", mode = READ, deprecatedBy = "gds.pipeline.exists")
    @Description(DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<PipelineExistsResult> betaExists(@Name(value = "pipelineName") String pipelineName) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.beta.pipeline.exists");
        executionContext()
            .log()
            .warn("The procedure `gds.beta.pipeline.exists` is deprecated and will be removed in a future release. Please use `gds.pipeline.exists` instead.");

        return exists(pipelineName);
    }

}
