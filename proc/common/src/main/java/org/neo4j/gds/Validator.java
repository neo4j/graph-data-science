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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;

public class Validator<CONFIG extends AlgoBaseConfig> {

    private final ValidationConfig<CONFIG> validationConfig;

    public Validator(ValidationConfig<CONFIG> validationConfig) {this.validationConfig = validationConfig;}

    public final void validateConfigsBeforeLoad(
        GraphCreateConfig graphCreateConfig,
        CONFIG config
    ) {
        validationConfig.validateConfigsBeforeLoad( graphCreateConfig, config);
    }

    public final void validateConfigWithGraphStore(
        GraphStore graphStore,
        GraphCreateConfig graphCreateConfig,
        CONFIG config
    ) {
        config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        );
        GraphStoreValidation.validate(graphStore, config);
        validationConfig.validateConfigsAfterLoad(graphStore, graphCreateConfig, config);
    }
}
