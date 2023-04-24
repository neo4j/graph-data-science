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

import org.neo4j.gds.core.write.NativeNodeLabelExporterBuilder;
import org.neo4j.gds.core.write.NativeNodePropertiesExporterBuilder;
import org.neo4j.gds.core.write.NativeRelationshipExporterBuilder;
import org.neo4j.gds.core.write.NativeRelationshipPropertiesExporterBuilder;
import org.neo4j.gds.core.write.NativeRelationshipStreamExporterBuilder;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

final class OpenGdsExportBuildersContextProvider extends LifecycleAdapter {

    private final GlobalProcedures globalProcedures;

    OpenGdsExportBuildersContextProvider(GlobalProcedures globalProcedures) {
        this.globalProcedures = globalProcedures;
    }

    @Override
    public void init() {
        globalProcedures.registerComponent(
            NodePropertyExporterBuilder.class,
            OpenGdsExportBuildersContextProvider::nativeNodePropertyExporterBuilder,
            true
        );

        globalProcedures.registerComponent(
            RelationshipStreamExporterBuilder.class,
            OpenGdsExportBuildersContextProvider::relationshipStreamExporterBuilder,
            true
        );

        globalProcedures.registerComponent(
            RelationshipExporterBuilder.class,
            OpenGdsExportBuildersContextProvider::relationshipExporterBuilder,
            true
        );

        globalProcedures.registerComponent(
            RelationshipPropertiesExporterBuilder.class,
            OpenGdsExportBuildersContextProvider::relationshipPropertiesExporterBuilder,
            true
        );

        globalProcedures.registerComponent(
            NodeLabelExporterBuilder.class,
            OpenGdsExportBuildersContextProvider::nativeNodeLabelExporterNodeLabelExporterBuilder,
            true
        );
    }

    private static RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder(Context ctx) {
        return new NativeRelationshipPropertiesExporterBuilder(transactionContext(ctx));
    }

    private static NodePropertyExporterBuilder nativeNodePropertyExporterBuilder(Context ctx) {
        return new NativeNodePropertiesExporterBuilder(transactionContext(ctx));
    }

    private static RelationshipStreamExporterBuilder relationshipStreamExporterBuilder(Context ctx) {
        return new NativeRelationshipStreamExporterBuilder(transactionContext(ctx));
    }

    private static RelationshipExporterBuilder relationshipExporterBuilder(Context ctx) {
        return new NativeRelationshipExporterBuilder(transactionContext(ctx));
    }

    private static NodeLabelExporterBuilder nativeNodeLabelExporterNodeLabelExporterBuilder(Context ctx) {
        return new NativeNodeLabelExporterBuilder(transactionContext(ctx));
    }

    private static TransactionContext transactionContext(Context ctx) {
        return DatabaseTransactionContext.of(ctx.graphDatabaseAPI(), ctx.internalTransactionOrNull());
    }
}
