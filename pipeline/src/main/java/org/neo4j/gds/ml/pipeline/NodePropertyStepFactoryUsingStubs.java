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

import java.util.List;
import java.util.Map;

final class NodePropertyStepFactoryUsingStubs {
    private static volatile NodePropertyStepFactoryUsingStubs INSTANCE = null;

    private final Map<CanonicalProcedureName, Object> supportedProcedures;

    private NodePropertyStepFactoryUsingStubs(Map<CanonicalProcedureName, Object> supportedProcedures) {
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
        String procedureName,
        Map<String, Object> configuration,
        List<String> contextNodeLabels,
        List<String> contextRelationshipTypes
    ) {
        // identify stub

        // parse/ validate

        // create step

        throw new UnsupportedOperationException("TODO: not populated yet");
    }

//    private static AlgoBaseConfig tryParsingConfig(
//        GdsCallableFinder.GdsCallableDefinition callableDefinition,
//        Map<String, Object> configuration
//    ) {
//        NewConfigFunction<AlgoBaseConfig> newConfigFunction = callableDefinition
//            .algorithmSpec()
//            .newConfigFunction();
//
//        var defaults = DefaultsConfiguration.Instance;
//        var limits = LimitsConfiguration.Instance;
//
//        // passing the EMPTY_USERNAME as we only try to check if the given configuration itself is valid
//        return new AlgoConfigParser<>(Username.EMPTY_USERNAME.username(), newConfigFunction, defaults, limits).processInput(configuration);
//    }
}
