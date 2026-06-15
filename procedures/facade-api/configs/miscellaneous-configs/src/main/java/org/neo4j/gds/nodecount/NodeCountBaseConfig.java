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
package org.neo4j.gds.nodecount;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;

/**
 * The configuration shared by every mode of the algorithm.
 * Node counting exposes no algorithm-specific settings, so it simply inherits the common options
 * (concurrency, node/relationship filters, ...) from {@link AlgoBaseConfig} and maps them onto the
 * algorithm {@link NodeCountParameters parameters}. A real algorithm would declare its own settings here.
 */
public interface NodeCountBaseConfig extends AlgoBaseConfig {

    @Configuration.Ignore
    default NodeCountParameters toParameters() {
        return new NodeCountParameters(concurrency());
    }
}
