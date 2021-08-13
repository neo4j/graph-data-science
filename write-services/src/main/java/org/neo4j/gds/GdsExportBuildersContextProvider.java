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
package org.neo4j.gds;

import org.neo4j.gds.core.TransactionContext;
import org.neo4j.gds.core.write.NativeNodePropertyExporter;
import org.neo4j.gds.core.write.NativeRelationshipStreamExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.NativeRelationshipExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporter;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

final class GdsExportBuildersContextProvider extends LifecycleAdapter {

    private final GlobalProcedures globalProcedures;

    GdsExportBuildersContextProvider(GlobalProcedures globalProcedures) {
        this.globalProcedures = globalProcedures;
    }

    @Override
    public void init() {
        globalProcedures.registerComponent(
            NodePropertyExporterBuilder.class,
            GdsExportBuildersContextProvider::nativeNodePropertyExporterBuilder,
            true
        );

        globalProcedures.registerComponent(
            RelationshipStreamExporterBuilder.class,
            GdsExportBuildersContextProvider::relationshipStreamExporterBuilder,
            true
        );

        globalProcedures.registerComponent(
            RelationshipExporterBuilder.class,
            GdsExportBuildersContextProvider::relationshipExporterBuilder,
            true
        );
    }

    private static NodePropertyExporterBuilder<NativeNodePropertyExporter> nativeNodePropertyExporterBuilder(Context ctx) {
        return new NativeNodePropertyExporter.Builder(transactionContext(ctx));
    }

    private static RelationshipStreamExporterBuilder<? extends RelationshipStreamExporter> relationshipStreamExporterBuilder(Context ctx) {
        return new NativeRelationshipStreamExporter.Builder(transactionContext(ctx));
    }

    private static RelationshipExporterBuilder<?> relationshipExporterBuilder(Context ctx) {
        return new NativeRelationshipExporter.Builder(transactionContext(ctx));
    }

    private static TransactionContext transactionContext(Context ctx) {
        return TransactionContext.of(ctx.graphDatabaseAPI(), ctx.internalTransactionOrNull());
    }
}
