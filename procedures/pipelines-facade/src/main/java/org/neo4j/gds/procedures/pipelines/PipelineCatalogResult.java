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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.ml.pipeline.TrainingPipeline;

import java.time.ZonedDateTime;
import java.util.Map;

public final class PipelineCatalogResult {
    public final Map<String, Object> pipelineInfo;
    public final String pipelineName;
    public final String pipelineType;
    public final ZonedDateTime creationTime;

    private PipelineCatalogResult(
        Map<String, Object> pipelineInfo,
        String pipelineName,
        String pipelineType,
        ZonedDateTime creationTime
    ) {
        this.pipelineInfo = pipelineInfo;
        this.pipelineName = pipelineName;
        this.pipelineType = pipelineType;
        this.creationTime = creationTime;
    }

    public static PipelineCatalogResult create(TrainingPipeline<?> pipeline, String pipelineName) {
        return new PipelineCatalogResult(pipeline.toMap(), pipelineName, pipeline.type(), pipeline.creationTime());
    }
}
