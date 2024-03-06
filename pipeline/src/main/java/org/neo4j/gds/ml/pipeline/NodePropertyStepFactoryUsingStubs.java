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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.procedures.AlgorithmsAndCatalogFacade;

import java.util.List;
import java.util.Map;

final class NodePropertyStepFactoryUsingStubs {
    private static volatile NodePropertyStepFactoryUsingStubs INSTANCE = null;

    private final Map<CanonicalProcedureName, Stub> supportedProcedures;

    private NodePropertyStepFactoryUsingStubs(Map<CanonicalProcedureName, Stub> supportedProcedures) {
        this.supportedProcedures = supportedProcedures;
    }

    /**
     * This is terrible, but necessary.
     * I am unable to change all the code around models to allow dependency injection. I tried and failed.
     * Will come back to it later, promise. I will bend our software to my will, eventually.
     * Here I need to get a handle on this stateful thing (state as in mappings of procedure name to executable code),
     * and I can't even rely on it existing, because model loading from Aura maintenance extension initialisation,
     * and building the GDS system in the other extension, can happen in arbitrary order. Like I said,
     * first move was to eliminate this problem by folding Aura maintenance extension into GDS extension; alas,
     * I was defeated.
     * Therefore, we capture those definitions in here: which procedure names to which stubs.
     * And we use it from several places.
     *
     * @param requestor something for debugging later
     */
    static NodePropertyStepFactoryUsingStubs GetOrCreate(String requestor) {
        // do people still do this double clutch singleton trick? Smells like to noughties :)
        if (INSTANCE == null) {
            synchronized (NodePropertyStepFactoryUsingStubs.class) {
                if (INSTANCE == null) {
                    INSTANCE = NodePropertyStepFactoryUsingStubs.create();
                }
            }
        }

        return INSTANCE;
    }

    private static NodePropertyStepFactoryUsingStubs create() {
        return new NodePropertyStepFactoryUsingStubs(Map.of());
    }

    /**
     * Short term integration thing where we interject in existing stuff,
     * for procedures that opt-in. Once all are opted in, this moves to be the default,
     * indeed the only thing.
     */
    boolean handles(String procedureName) {
        var canonicalProcedureName = CanonicalProcedureName.parse(procedureName);

        return supportedProcedures.containsKey(canonicalProcedureName);
    }

    ExecutableNodePropertyStep createNodePropertyStep(
        AlgorithmsAndCatalogFacade facade, String procedureName,
        Map<String, Object> configuration,
        List<String> contextNodeLabels,
        List<String> contextRelationshipTypes
    ) {
        var canonicalProcedureName = CanonicalProcedureName.parse(procedureName);

        // identify stub
        var stub = supportedProcedures.get(canonicalProcedureName);

        // parse/ validate
        stub.validateBeforeCreatingNodePropertyStep(facade, configuration);

        // create step
        return new StubPoweredNodePropertyStep(
            canonicalProcedureName,
            configuration,
            contextNodeLabels,
            contextRelationshipTypes
        );
    }
}
