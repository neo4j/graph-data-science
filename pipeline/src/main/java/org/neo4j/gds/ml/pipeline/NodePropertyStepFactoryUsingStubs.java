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

import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.procedures.algorithms.CanonicalProcedureName;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;

import java.util.List;
import java.util.Map;

final class NodePropertyStepFactoryUsingStubs {
    private final MutateModeAlgorithmLibrary mutateModeAlgorithmLibrary = MutateModeAlgorithmLibrary.create();

    private static volatile NodePropertyStepFactoryUsingStubs INSTANCE = null;

    private final StubbyHolder stubbyHolder = new StubbyHolder();

    private final ValidationService validationService;

    private NodePropertyStepFactoryUsingStubs(ValidationService validationService) {
        this.validationService = validationService;
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
     */
    static NodePropertyStepFactoryUsingStubs GetOrCreate() {
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
        // not great, one day these should be injected
        var configurationParser = new ConfigurationParser(DefaultsConfiguration.Instance, LimitsConfiguration.Instance);
        var validationService = new ValidationService(
            DefaultsConfiguration.Instance,
            LimitsConfiguration.Instance,
            configurationParser
        );

        return new NodePropertyStepFactoryUsingStubs(validationService);
    }

    /**
     * Short term integration thing where we interject in existing stuff,
     * for procedures that opt-in. Once all are opted in, this moves to be the default,
     * indeed the only thing.
     */
    boolean handles(String procedureName) {
        var canonicalProcedureName = CanonicalProcedureName.parse(procedureName);

        return mutateModeAlgorithmLibrary.contains(canonicalProcedureName);
    }

    ExecutableNodePropertyStep createNodePropertyStep(
        String procedureName,
        Map<String, Object> configuration,
        List<String> contextNodeLabels,
        List<String> contextRelationshipTypes
    ) {
        /*
         * An explanation is in order here. User comes along with a string, and we need to match that to an algorithm.
         * So we do normalising, and a lookup.
         * The next thing we want to do is make things pretty, so we take the algorithm and look up its pretty name,
         * its canonical name. That's handy for error messages and consistency.
         */
        var normalisedProcedureName = CanonicalProcedureName.parse(procedureName);
        var algorithm = mutateModeAlgorithmLibrary.lookup(normalisedProcedureName);

        // The invariant is, you would have called "handles" above first, and you would have been turned away there
        if (algorithm == null) throw new IllegalStateException(
            "If you managed to get here, there was a programmer error somewhere");

        var canonicalProcedureName = MutateModeAlgorithmLibrary.algorithmToName(algorithm);

        validationService.validate(algorithm, configuration);

        // create step
        return new StubPoweredNodePropertyStep(
            canonicalProcedureName,
            configuration,
            contextNodeLabels,
            contextRelationshipTypes,
            algorithm
        );
    }

    Stub getStub(String procedureName) {
        var canonicalProcedureName = CanonicalProcedureName.parse(procedureName);

        var algorithm = mutateModeAlgorithmLibrary.lookup(canonicalProcedureName);

        return stubbyHolder.get(algorithm);
    }
}
