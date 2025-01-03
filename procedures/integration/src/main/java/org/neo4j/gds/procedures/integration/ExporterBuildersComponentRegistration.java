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

import org.neo4j.gds.core.write.ExportBuildersProvider;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

public class ExporterBuildersComponentRegistration {
    private final GlobalProcedures globalProcedures;

    public ExporterBuildersComponentRegistration(GlobalProcedures globalProcedures) {
        this.globalProcedures = globalProcedures;
    }

    public void registerExporterBuilders(ExportBuildersProvider exportBuildersProvider) {
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
