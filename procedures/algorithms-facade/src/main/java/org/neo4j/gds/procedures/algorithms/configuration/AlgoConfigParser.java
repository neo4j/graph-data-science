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
package org.neo4j.gds.procedures.algorithms.configuration;

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitViolation;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class AlgoConfigParser<CONFIG extends AlgoBaseConfig> implements ProcConfigParser<CONFIG> {
    private final NewConfigFunction<CONFIG> newConfigFunction;
    private final String username;
    private final DefaultsConfiguration defaults;
    private final LimitsConfiguration limits;

    public AlgoConfigParser(
        String username,
        NewConfigFunction<CONFIG> newConfigFunction,
        DefaultsConfiguration defaults,
        LimitsConfiguration limits
    ) {
        this.username = username;
        this.newConfigFunction = newConfigFunction;
        this.defaults = defaults;
        this.limits = limits;
    }

    @Override
    public CONFIG processInput(Map<String, Object> configuration) {
        // apply defaults
        var inputWithDefaults = applyDefaults(configuration, username);
        // parse configuration
        CONFIG algorithmConfiguration = produceConfig(inputWithDefaults);
        //validate the original config for extra-added parameters
        validateOriginalConfig(configuration, algorithmConfiguration.configKeys());
        //ensure that limits are fine
        validateLimits(algorithmConfiguration, inputWithDefaults);

        return algorithmConfiguration;
    }

    private HashSet<String> getIrrelevantInputtedKeys(Map<String, Object> configuration, HashSet<String> allowedKeys) {
        var irrelevantInputtedKeys = new HashSet<>(configuration.keySet());
        irrelevantInputtedKeys.removeAll(allowedKeys);
        return irrelevantInputtedKeys;
    }

    private HashMap<String, Object> getConfigurationForLimitValidation(
        Map<String, Object> configuration,
        HashSet<String> irrelevantInputtedKeys
    ) {
        var configurationButWithIrrelevantInputtedKeysRemoved = new HashMap<>(configuration);
        configurationButWithIrrelevantInputtedKeysRemoved.keySet().removeAll(irrelevantInputtedKeys);
        return configurationButWithIrrelevantInputtedKeysRemoved;
    }

    private void validateLimits(
        CONFIG algorithmConfiguration,
        Map<String, Object> userInputWithDefaults
    ) throws IllegalArgumentException {

        // handle limits
        var allowedKeys = new HashSet<>(algorithmConfiguration.configKeys());
        var irrelevantInputtedKeys = getIrrelevantInputtedKeys(userInputWithDefaults, allowedKeys);
        var configurationButWithIrrelevantInputtedKeysRemoved = getConfigurationForLimitValidation(
            userInputWithDefaults,
            irrelevantInputtedKeys
        ); //remove any useless configuration parameters e.g., sourceNode for Wcc

        validateLimits(configurationButWithIrrelevantInputtedKeysRemoved);

    }


    private void validateLimits(HashMap<String, Object> configurationButWithIrrelevantInputtedKeysRemoved) {
        var violations = limits.validate(configurationButWithIrrelevantInputtedKeysRemoved, username);

        if (violations.isEmpty()) return;

        var violationMessages = new LinkedList<String>();
        var delimeter = "\n";
        if (violations.size() > 1) {
            violationMessages.add("Configuration exceeded multiple limits:");
            delimeter = "\n - ";
        }

        violations.stream()
            .map(LimitViolation::getErrorMessage)
            .sorted()
            .forEach(violationMessages::add);

        throw new IllegalArgumentException(String.join(delimeter, violationMessages));
    }

    private CONFIG produceConfig(Map<String, Object> configurationWithDefaultsApplied) {
        CypherMapWrapper cypherMapWrapper = CypherMapWrapper.create(configurationWithDefaultsApplied);
        return newConfigFunction.apply(username, cypherMapWrapper);
    }


    private void validateOriginalConfig(
        Map<String, Object> configuration,
        Collection<String> allowedConfigKeys
    ) throws IllegalArgumentException {
        Map<String, Object> newConfiguration = new HashMap<>(configuration);

        CypherMapWrapper.create(newConfiguration)
            .requireOnlyKeysFrom(allowedConfigKeys); //ensure user has not included any  incorrect params
    }

    private Map<String, Object> applyDefaults(Map<String, Object> configuration, String username) {
        // apply defaults
        return defaults.apply(configuration, username);
    }
}
