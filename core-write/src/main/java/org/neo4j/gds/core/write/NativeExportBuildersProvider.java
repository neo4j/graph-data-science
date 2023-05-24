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

import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.gds.transaction.TransactionContext;

public final class NativeExportBuildersProvider implements ExportBuildersProvider {

    @Override
    public NodePropertyExporterBuilder nodePropertyExporterBuilder(ExporterContext ctx) {
        return new NativeNodePropertiesExporterBuilder(transactionContext(ctx));
    }

    @Override
    public RelationshipStreamExporterBuilder relationshipStreamExporterBuilder(ExporterContext ctx) {
        return new NativeRelationshipStreamExporterBuilder(transactionContext(ctx));
    }

    @Override
    public RelationshipExporterBuilder relationshipExporterBuilder(ExporterContext ctx) {
        return new NativeRelationshipExporterBuilder(transactionContext(ctx));
    }

    @Override
    public RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder(ExporterContext ctx) {
        return new NativeRelationshipPropertiesExporterBuilder(transactionContext(ctx));
    }

    @Override
    public NodeLabelExporterBuilder nodeLabelExporterBuilder(ExporterContext ctx) {
        return new NativeNodeLabelExporterBuilder(transactionContext(ctx));
    }

    private static TransactionContext transactionContext(ExporterContext ctx) {
        return DatabaseTransactionContext.of(ctx.graphDatabaseAPI(), ctx.internalTransaction());
    }

}
