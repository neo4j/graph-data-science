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

import org.neo4j.gds.api.User;
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
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ConfigurationParser {
    private final DefaultsConfiguration defaults;
    private final LimitsConfiguration limits;

    public ConfigurationParser(DefaultsConfiguration defaults, LimitsConfiguration limits) {
        this.defaults = defaults;
        this.limits = limits;
    }


    public <CONFIG extends AlgoBaseConfig> CONFIG parseConfiguration(
        Map<String, Object> configuration,
       Function<CypherMapWrapper, CONFIG> lexer,
        User user
    ) {

       return parseConfiguration(configuration, (__, cypherMapWrapper) -> lexer.apply(cypherMapWrapper),user);
    }

    /**
     * Convenient configuration parsing using globally configured defaults and limits
     */
    <CONFIG extends AlgoBaseConfig> CONFIG parseConfiguration(
        Map<String, Object> configuration,
        BiFunction<String, CypherMapWrapper, CONFIG> lexer,
        User user
    ) {
        return parseConfiguration(defaults, limits, user.getUsername(), configuration, lexer);
    }

   public  <CONFIG extends AlgoBaseConfig> CONFIG parseConfigurationWithoutDefaultsAndLimits(
        Map<String, Object> configuration,
        BiFunction<String, CypherMapWrapper, CONFIG> lexer,
        String username
    ) {
        return parseConfiguration(DefaultsConfiguration.Empty, LimitsConfiguration.Empty, username, configuration, lexer);
    }

    /**
     * Configuration parsing using directly configured defaults and limits
     */
    private  <CONFIG extends AlgoBaseConfig> CONFIG parseConfiguration(
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        String username,
        Map<String, Object> rawConfiguration,
        BiFunction<String, CypherMapWrapper, CONFIG> lexer
    ) {
        var configurationWithDefaultsAdded = applyDefaults(rawConfiguration, username, defaultsConfiguration);

        var cypherMapWrapper = CypherMapWrapper.create(configurationWithDefaultsAdded);

        var configuration = lexer.apply(username, cypherMapWrapper);

        validateOriginalConfiguration(rawConfiguration, configuration.configKeys());

        validateLimits(configuration, username, configurationWithDefaultsAdded, limitsConfiguration);

        return configuration;
    }

    Map<String, Object> applyDefaults(
        Map<String, Object> configuration, String username,
        DefaultsConfiguration defaultsConfiguration
    ) {
        return defaultsConfiguration.apply(configuration, username);
    }

    /**
     * @throws java.lang.IllegalArgumentException if user has included any incorrect parameters
     */
    void validateOriginalConfiguration(Map<String, Object> configuration, Collection<String> allowedConfigKeys) {
        CypherMapWrapper.create(configuration).requireOnlyKeysFrom(allowedConfigKeys);
    }

    <CONFIGURATION extends AlgoBaseConfig> void validateLimits(
        CONFIGURATION algorithmConfiguration,
        String username,
        Map<String, Object> userInputWithDefaults,
        LimitsConfiguration limitsConfiguration
    ) throws IllegalArgumentException {
        var allowedKeys = new HashSet<>(algorithmConfiguration.configKeys());
        var irrelevantInputtedKeys = getIrrelevantInputtedKeys(userInputWithDefaults, allowedKeys);
        var configurationButWithIrrelevantInputtedKeysRemoved = getConfigurationForLimitValidation(
            userInputWithDefaults,
            irrelevantInputtedKeys
        ); //remove any useless configuration parameters e.g., sourceNode for Wcc

        validateLimits(configurationButWithIrrelevantInputtedKeysRemoved, username, limitsConfiguration);
    }

    private Set<String> getIrrelevantInputtedKeys(
        Map<String, Object> configuration,
        Collection<String> allowedKeys
    ) {
        var irrelevantInputtedKeys = new HashSet<>(configuration.keySet());
        irrelevantInputtedKeys.removeAll(allowedKeys);
        return irrelevantInputtedKeys;
    }

    private Map<String, Object> getConfigurationForLimitValidation(
        Map<String, Object> configuration,
        Collection<String> irrelevantInputtedKeys
    ) {
        var configurationButWithIrrelevantInputtedKeysRemoved = new HashMap<>(configuration);
        configurationButWithIrrelevantInputtedKeysRemoved.keySet().removeAll(irrelevantInputtedKeys);
        return configurationButWithIrrelevantInputtedKeysRemoved;
    }

    private void validateLimits(
        Map<String, Object> configurationButWithIrrelevantInputtedKeysRemoved,
        String username,
        LimitsConfiguration limitsConfiguration
    ) {
        var violations = limitsConfiguration.validate(configurationButWithIrrelevantInputtedKeysRemoved, username);

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
}
