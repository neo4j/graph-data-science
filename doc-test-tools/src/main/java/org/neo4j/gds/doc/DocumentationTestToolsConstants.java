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
package org.neo4j.gds.doc;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

final class DocumentationTestToolsConstants {
    // so this traces via Gradle, where we copy contents from the :doc project into the build directory for this project
    static final Path ASCIIDOC_PATH = Paths.get("build/doc-sources/modules/ROOT");

    static final NumberFormat FLOAT_FORMAT;

    static {
        var decimalFormat = DecimalFormat.getInstance(Locale.ENGLISH);
        decimalFormat.setMaximumFractionDigits(15);
        decimalFormat.setGroupingUsed(false);
        FLOAT_FORMAT = decimalFormat;
    }

    // ***
    // Grammar bits below here
    // ***

    static final String CODE_BLOCK_CONTEXT = ":listing";
    static final String TABLE_CONTEXT = ":table";
    static final String SETUP_QUERY_ROLE = "setup-query";
    static final String GRAPH_PROJECT_QUERY_ROLE = "graph-project-query";
    static final String QUERY_EXAMPLE_ROLE = "query-example";
    static final String TEST_TYPE_NO_RESULT = "no-result";
    static final String TEST_GROUP_ATTRIBUTE = "group";
    static final String TEST_OPERATOR_ATTRIBUTE = "operator";
    static final String ROLE_SELECTOR = "role";

    private DocumentationTestToolsConstants() {}
}
