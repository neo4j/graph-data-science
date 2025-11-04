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
package org.neo4j.gds.doc.syntax;

import org.neo4j.gds.articulationpoints.ArticulationPointsMutateConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsStatsConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsStreamConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsWriteConfig;

import java.util.List;

class ArticulationPointsSyntaxTest extends SyntaxTestBase {

    protected Iterable<SyntaxModeMeta> syntaxModes() {
        return List.of(
            SyntaxModeMeta.of(SyntaxMode.STREAM, ArticulationPointsStreamConfig.class),
            SyntaxModeMeta.of(SyntaxMode.STATS, ArticulationPointsStatsConfig.class),
            SyntaxModeMeta.of(SyntaxMode.MUTATE, ArticulationPointsMutateConfig.class),
            SyntaxModeMeta.of(SyntaxMode.WRITE, ArticulationPointsWriteConfig.class)
        );
    }

    @Override
    protected String adocFile() {
        return "pages/algorithms/articulation-points.adoc";
    }
}
