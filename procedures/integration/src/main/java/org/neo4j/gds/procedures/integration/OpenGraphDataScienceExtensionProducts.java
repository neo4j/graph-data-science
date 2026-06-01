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

import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;

/**
 * There are some singleton products that are created in the builder,
 * which we need to make available for down stream construction efforts.
 */
public class OpenGraphDataScienceExtensionProducts {
    private final OpenGraphDataScienceExtensionBuilder graphDataScienceExtensionBuilder;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final TaskStoreService taskStoreService;

    OpenGraphDataScienceExtensionProducts(
        OpenGraphDataScienceExtensionBuilder graphDataScienceExtensionBuilder,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TaskStoreService taskStoreService
    ) {
        this.graphDataScienceExtensionBuilder = graphDataScienceExtensionBuilder;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.taskStoreService = taskStoreService;
    }

    public OpenGraphDataScienceExtensionBuilder graphDataScienceExtensionBuilder() {
        return graphDataScienceExtensionBuilder;
    }

    public TaskRegistryFactoryService taskRegistryFactoryService() {
        return taskRegistryFactoryService;
    }

    public TaskStoreService taskStoreService() {
        return taskStoreService;
    }
}
