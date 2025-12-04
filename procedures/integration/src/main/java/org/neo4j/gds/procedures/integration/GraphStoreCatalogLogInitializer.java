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
package org.neo4j.gds.procedures.integration;

import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * On plugin init, we need to initialize the log on {@link org.neo4j.gds.core.loading.GraphStoreCatalog}.
 * But not before, I think. Unsure. This was how it was originally :shrug:
 */
class GraphStoreCatalogLogInitializer extends LifecycleAdapter {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;

    GraphStoreCatalogLogInitializer(Log log, GraphStoreCatalogService graphStoreCatalogService) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    @Override
    public void init() {
        graphStoreCatalogService.setLog(log);
    }
}
