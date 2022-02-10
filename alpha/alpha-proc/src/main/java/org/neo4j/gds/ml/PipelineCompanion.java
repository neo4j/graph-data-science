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
package org.neo4j.gds.ml;

import java.util.Map;

public final class PipelineCompanion {

    private PipelineCompanion() {}

    public static void prepareTrainConfig(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        // TODO: this will go away once node property steps do not modify the graph store with the given graphName
        //  In the future it might operate on a shallow copy instead.
        if (graphNameOrConfiguration instanceof String) {
            algoConfiguration.put("graphName", graphNameOrConfiguration);
        } else {
            algoConfiguration.put("graphName", "__ANONYMOUS_GRAPH__");
        }
    }
}
