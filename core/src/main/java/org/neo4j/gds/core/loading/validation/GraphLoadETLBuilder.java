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
package org.neo4j.gds.core.loading.validation;

import org.neo4j.gds.config.AlgoBaseConfig;

import java.util.LinkedList;
import java.util.List;

public final class GraphLoadETLBuilder {
    private final List<GraphStoreLoadedETL> graphStoreLoadedHooks = new LinkedList<>();
    private final List<GraphStoreFilteredETL> graphStoreFilteredHooks = new LinkedList<>();
    private final List<GraphLoadedETL> graphLoadedHooks = new LinkedList<>();

    private GraphLoadETLBuilder() {

    }

    /**
     * This is the only way into this class.
     * It helps us remember to include the behaviour that sits on the configuration class.
     */
    public static GraphLoadETLBuilder from(AlgoBaseConfig configuration) {
        var etlStep = new GraphStoreFilteredETLFromConfiguration(configuration);

        return new GraphLoadETLBuilder().withGraphStoreFilteredETL(etlStep);
    }

    /**
     * @deprecated short term shunt, don't use it
     */
    @Deprecated
    public static GraphLoadETLBuilder fromNothing() {
        return new GraphLoadETLBuilder();
    }

    public GraphLoadETLBuilder withGraphStoreLoadedETL(GraphStoreLoadedETL hook) {
        graphStoreLoadedHooks.add(hook);
        return this;
    }

    public GraphLoadETLBuilder withGraphStoreFilteredETL(GraphStoreFilteredETL hook) {
        graphStoreFilteredHooks.add(hook);
        return this;
    }

    public GraphLoadETLBuilder withGraphLoadedETL(GraphLoadedETL hook) {
        graphLoadedHooks.add(hook);
        return this;
    }

    public GraphLoadETL build() {
        return new GraphLoadETL(graphStoreLoadedHooks, graphStoreFilteredHooks, graphLoadedHooks);
    }
}
