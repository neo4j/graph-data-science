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
package org.neo4j.gds.config;

import org.neo4j.gds.annotation.Configuration;

import java.io.Serializable;

import static org.neo4j.gds.config.StringIdentifierValidations.replaceBlankWithNull;
import static org.neo4j.gds.config.StringIdentifierValidations.validateNoWhiteCharacter;

@Configuration
public interface ModelConfig extends Serializable, BaseConfig {

    long serialVersionUID = 0x42L;

    String MODEL_NAME_KEY = "modelName";
    String MODEL_TYPE_KEY = "modelType";

    @Configuration.ConvertWith("validateName")
    String modelName();

    static String validateName(String input) {
        return validateNoWhiteCharacter(replaceBlankWithNull(input), MODEL_TYPE_KEY);
    }
}
