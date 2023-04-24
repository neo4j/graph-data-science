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
package org.neo4j.gds.projection;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.compat.CompatUserAggregationFunction;
import org.neo4j.gds.compat.CompatUserAggregator;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.Username;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import java.util.List;

import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;

public class CypherAggregation implements CompatUserAggregationFunction {
    // NOTE: keep in sync with `GraphAggregator.procedureSyntax`
    static final QualifiedName FUNCTION_NAME = new QualifiedName(
        new String[]{"gds", "alpha", "graph"},
        "project"
    );

    public static CompatUserAggregationFunction newInstance() {
        return new CypherAggregation();
    }

    @Override
    public UserFunctionSignature signature() {
        return Neo4jProxy.userFunctionSignature(
            FUNCTION_NAME,
            // input signature:
            List.of(
                // @Name("graphName") TextValue graphName
                FieldSignature.inputField("graphName", Neo4jTypes.NTString),
                // @Name("sourceNode") AnyValue sourceNode
                FieldSignature.inputField("sourceNode", Neo4jTypes.NTAny),
                // @Name(value = "targetNode", defaultValue = "null") AnyValue targetNode
                FieldSignature.inputField("targetNode", Neo4jTypes.NTAny, nullValue(Neo4jTypes.NTAny)),
                // @Name(value = "nodesConfig", defaultValue = "null") AnyValue nodesConfig
                FieldSignature.inputField("nodesConfig", Neo4jTypes.NTAny, nullValue(Neo4jTypes.NTAny)),
                // @Name(value = "relationshipConfig", defaultValue = "null") AnyValue relationshipConfig
                FieldSignature.inputField("relationshipConfig", Neo4jTypes.NTAny, nullValue(Neo4jTypes.NTAny)),
                // @Name(value = "configuration", defaultValue = "null") AnyValue config
                FieldSignature.inputField("configuration", Neo4jTypes.NTAny, nullValue(Neo4jTypes.NTAny))
            ),
            // output type: Map
            Neo4jTypes.NTMap,
            // function description
            "Creates a named graph in the catalog for use by algorithms.",
            // not internal
            false,
            // thread-safe, yes please
            true
        );
    }

    @Override
    public CompatUserAggregator create(Context ctx) throws ProcedureException {
        // TODO: reuse AuraProcedureUtil

        var procedures = GraphDatabaseApiProxy.resolveDependency(
            ctx.dependencyResolver(),
            GlobalProcedures.class
        );
        var databaseService = procedures
            .lookupComponentProvider(GraphDatabaseService.class, true)
            .apply(ctx);

        var username = procedures.lookupComponentProvider(Username.class, true).apply(ctx);
        var runsOnCompositeDatabase = Neo4jProxy.isCompositeDatabase(databaseService);

        return new GraphAggregator(
            DatabaseId.of(databaseService),
            username.username(),
            !runsOnCompositeDatabase
        );
    }
}
