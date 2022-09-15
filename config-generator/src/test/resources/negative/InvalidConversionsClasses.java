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
package negative;

import org.neo4j.gds.annotation.Configuration;

@Configuration("InvalidConversionsClassesConfig")
public interface InvalidConversionsClasses {

    @Configuration.ConvertWith(method = "")
    int emptyConverter();

    @Configuration.ConvertWith(method = "multipleOverloads")
    int multi();

    static int multipleOverloads(String input) {
        return 42;
    }

    static int multipleOverloads(long input) {
        return 42;
    }

    @Configuration.ConvertWith(method = "negative.class.does.not.exist#foo")
    int classDoesNotExist();

    @Configuration.ConvertWith(method = "methodDoesNotExist")
    int converterMethodDoesNotExist();

    @Configuration.ConvertWith(method = "negative.InvalidConversionsClasses#methodDoesNotExist")
    int fullQualifiedConverterMethodDoesNotExist();

    @Configuration.ConvertWith(method = "negative.InvalidConversionsClasses#")
    int missingMethodName();

    @Configuration.ConvertWith(method = "negative.InvalidConversionsClasses.invalidIdentifier")
    int dotMakesIdentifierInvalid();

    @Configuration.ConvertWith(method = "negative.invalid identifier")
    int spaceMakesIdentifierInvalid();
}
