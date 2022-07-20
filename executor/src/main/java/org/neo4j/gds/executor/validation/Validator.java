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
package org.neo4j.gds.executor.validation;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;

public class Validator<CONFIG extends AlgoBaseConfig> {

    private final ValidationConfiguration<CONFIG> validationConfiguration;

    public Validator(ValidationConfiguration<CONFIG> validationConfiguration) {
        this.validationConfiguration = validationConfiguration;
    }

    public void validateConfigsBeforeLoad(GraphProjectConfig graphProjectConfig, CONFIG config) {
        validationConfiguration
            .beforeLoadValidations()
            .forEach(validation -> validation.validateConfigsBeforeLoad(graphProjectConfig, config));
    }

    public void validateConfigWithGraphStore(
        GraphStore graphStore,
        GraphProjectConfig graphProjectConfig,
        CONFIG config
    ) {
        config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        );

        validationConfiguration
            .afterLoadValidations()
            .forEach(validation -> validation.validateConfigsAfterLoad(graphStore, graphProjectConfig, config));
    }
}
