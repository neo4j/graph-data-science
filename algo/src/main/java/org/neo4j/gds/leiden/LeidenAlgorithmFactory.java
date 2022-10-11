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
package org.neo4j.gds.leiden;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;

public class LeidenAlgorithmFactory<CONFIG extends LeidenBaseConfig> extends GraphAlgorithmFactory<Leiden, CONFIG> {
    @Override
    public Leiden build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {

        var seedValues = Optional
            .ofNullable(configuration.seedProperty()).map(graph::nodeProperties).orElse(null);

        return new Leiden(
            graph,
            configuration.maxLevels(),
            configuration.gamma(),
            configuration.theta(),
            configuration.includeIntermediateCommunities(),
            configuration.randomSeed().orElse(0L),
            seedValues,
            configuration.concurrency(),
            progressTracker
        );
    }

    @Override
    public String taskName() {
        return "Leiden";
    }
}
