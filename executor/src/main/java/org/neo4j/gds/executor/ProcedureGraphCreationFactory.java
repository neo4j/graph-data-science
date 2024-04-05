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
package org.neo4j.gds.executor;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreLoader;
import org.neo4j.gds.applications.graphstorecatalog.MemoryUsageValidator;
import org.neo4j.gds.config.AlgoBaseConfig;

import java.util.function.BiFunction;

public class ProcedureGraphCreationFactory<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig
> implements GraphCreationFactory<ALGO, ALGO_RESULT, CONFIG> {

    private final BiFunction<CONFIG, String, GraphStoreLoader> graphStoreLoaderFn;
    private final MemoryUsageValidator memoryUsageValidator;

    public ProcedureGraphCreationFactory(
        BiFunction<CONFIG, String, GraphStoreLoader> graphStoreLoaderFn,
        MemoryUsageValidator memoryUsageValidator
    ) {
        this.graphStoreLoaderFn = graphStoreLoaderFn;
        this.memoryUsageValidator = memoryUsageValidator;
    }

    @Override
    public GraphCreation<ALGO, ALGO_RESULT, CONFIG> create(CONFIG config, String graphName) {
        return new ProcedureGraphCreation<>(graphStoreLoaderFn.apply(config, graphName), memoryUsageValidator, config);
    }
}
