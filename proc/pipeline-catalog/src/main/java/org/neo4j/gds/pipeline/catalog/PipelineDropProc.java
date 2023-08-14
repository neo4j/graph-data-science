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
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class PipelineDropProc extends BaseProc {

    private static final String DESCRIPTION = "Drops a pipeline and frees up the resources it occupies.";

    @Procedure(name = "gds.pipeline.drop", mode = READ)
    @Description(DESCRIPTION)
    public Stream<PipelineCatalogResult> drop(
        @Name(value = "pipelineName") String pipelineName,
        @Name(value = "failIfMissing", defaultValue = "true") boolean failIfMissing
    ) {
        CypherMapAccess.failOnBlank("pipelineName", pipelineName);

        TrainingPipeline<?> pipeline = null;

        if (failIfMissing) {
            pipeline = PipelineCatalog.drop(username(), pipelineName);
        } else {
            if (PipelineCatalog.exists(username(), pipelineName)) {
                pipeline = PipelineCatalog.drop(username(), pipelineName);
            }
        }

        return Stream.ofNullable(pipeline).map(pipe -> new PipelineCatalogResult(pipe, pipelineName));
    }

    @Procedure(name = "gds.beta.pipeline.drop", mode = READ, deprecatedBy = "gds.pipeline.drop")
    @Description(DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<PipelineCatalogResult> betaDrop(
        @Name(value = "pipelineName") String pipelineName,
        @Name(value = "failIfMissing", defaultValue = "true") boolean failIfMissing
    ) {
        executionContext()
            .log()
            .warn("Procedure `gds.beta.pipeline.drop` has been deprecated, please use `gds.pipeline.drop`.");

        return drop(pipelineName, failIfMissing);
    }
}
