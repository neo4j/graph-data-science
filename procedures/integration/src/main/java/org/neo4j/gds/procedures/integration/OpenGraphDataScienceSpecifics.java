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

import org.neo4j.gds.LicenseState;
import org.neo4j.gds.core.IdMapBehavior;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;

/**
 * Yet another [Parameter Object](https://wiki.c2.com/?ParameterObject).
 * We encapsulate the bits that vary between editions, a nice thing for grok-ability.
 * We establish a marker and separator, to distinguish from other plumbing bits.
 * And we greatly shorten parameter lists.
 */
public class OpenGraphDataScienceSpecifics {
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final IdMapBehavior idMapBehavior;
    private final LicenseState licenseState;
    private final Metrics metrics;
    private final ModelCatalog modelCatalog;

    public OpenGraphDataScienceSpecifics(
        ExporterBuildersProviderService exporterBuildersProviderService,
        IdMapBehavior idMapBehavior,
        LicenseState licenseState,
        Metrics metrics,
        ModelCatalog modelCatalog
    ) {
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.idMapBehavior = idMapBehavior;
        this.licenseState = licenseState;
        this.metrics = metrics;
        this.modelCatalog = modelCatalog;
    }

    ExporterBuildersProviderService exporterBuildersProviderService() {
        return exporterBuildersProviderService;
    }

    IdMapBehavior idMapBehavior() {
        return idMapBehavior;
    }

    LicenseState licenseState() {
        return licenseState;
    }

    Metrics metrics() {
        return metrics;
    }

    ModelCatalog modelCatalog() {
        return modelCatalog;
    }
}
