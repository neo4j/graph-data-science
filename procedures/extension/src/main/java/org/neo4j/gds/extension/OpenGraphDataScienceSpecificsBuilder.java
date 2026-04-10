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
package org.neo4j.gds.extension;

import org.neo4j.gds.OpenGdsLicenseState;
import org.neo4j.gds.concurrency.OpenGdsPoolSizes;
import org.neo4j.gds.core.OpenGdsIdMapBehavior;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.write.NativeExportBuildersProvider;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.integration.OpenGraphDataScienceSpecifics;

/**
 * Here we capture the choices for the open edition of GDS.
 */
class OpenGraphDataScienceSpecificsBuilder {
    OpenGraphDataScienceSpecifics build() {
        ExporterBuildersProviderService exporterBuildersProviderService = (__, ___) -> new NativeExportBuildersProvider(); // we always just offer native writes in OpenGDS

        var idMapBehavior = new OpenGdsIdMapBehavior();

        var licenseState = OpenGdsLicenseState.INSTANCE;

        var metrics = Metrics.DISABLED; // no metrics in OpenGDS

        var modelCatalog = new OpenModelCatalog();

        var poolSizes = new OpenGdsPoolSizes(); // limited to four

        return new OpenGraphDataScienceSpecifics(
            exporterBuildersProviderService,
            idMapBehavior,
            licenseState,
            metrics,
            modelCatalog,
            poolSizes
        );
    }
}
