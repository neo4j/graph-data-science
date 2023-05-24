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
package org.neo4j.gds.core.write;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public abstract class AbstractExportBuildersExtension extends
    ExtensionFactory<AbstractExportBuildersExtension.Dependencies> {

    protected AbstractExportBuildersExtension() {
        super(ExtensionType.DATABASE, "gds.write-services");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        var exportBuildersProviderSelector = exportBuildersProviderSelector(
            dependencies.graphDatabaseService(),
            dependencies.config()
        );
        return new GlobalProceduresExporterComponentProvider(
            dependencies.globalProcedures(),
            exportBuildersProviderSelector
        );
    }

    protected abstract ExportBuildersProviderSelector exportBuildersProviderSelector(
        GraphDatabaseService graphDatabaseService,
        Config config
    );

    static class GlobalProceduresExporterComponentProvider extends LifecycleAdapter {

        private final GlobalProcedures globalProcedures;
        private final ExportBuildersProviderSelector exportBuildersProviderSelector;

        GlobalProceduresExporterComponentProvider(
            GlobalProcedures globalProcedures,
            ExportBuildersProviderSelector exportBuildersProviderSelector
        ) {
            this.globalProcedures = globalProcedures;
            this.exportBuildersProviderSelector = exportBuildersProviderSelector;
        }

        @Override
        public void init() {
            var exportBuildersProvider = exportBuildersProviderSelector.select();
            globalProcedures.registerComponent(
                NodePropertyExporterBuilder.class,
                (ctx) -> exportBuildersProvider.nodePropertyExporterBuilder(
                    new ExporterContext.ProcedureContextWrapper(ctx)
                ),
                true
            );

            globalProcedures.registerComponent(
                RelationshipStreamExporterBuilder.class,
                (ctx) -> exportBuildersProvider.relationshipStreamExporterBuilder(
                    new ExporterContext.ProcedureContextWrapper(ctx)
                ),
                true
            );

            globalProcedures.registerComponent(
                RelationshipExporterBuilder.class,
                (ctx) -> exportBuildersProvider.relationshipExporterBuilder(
                    new ExporterContext.ProcedureContextWrapper(ctx)
                ),
                true
            );

            globalProcedures.registerComponent(
                RelationshipPropertiesExporterBuilder.class,
                (ctx) -> exportBuildersProvider.relationshipPropertiesExporterBuilder(
                    new ExporterContext.ProcedureContextWrapper(ctx)
                ),
                true
            );

            globalProcedures.registerComponent(
                NodeLabelExporterBuilder.class,
                (ctx) -> exportBuildersProvider.nodeLabelExporterBuilder(
                    new ExporterContext.ProcedureContextWrapper(ctx)
                ),
                true
            );
        }
    }

    public interface Dependencies {
        GlobalProcedures globalProcedures();

        Config config();

        GraphDatabaseService graphDatabaseService();
    }
}
