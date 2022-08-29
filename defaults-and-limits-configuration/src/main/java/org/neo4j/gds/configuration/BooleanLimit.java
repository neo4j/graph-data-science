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
package org.neo4j.gds.configuration;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class BooleanLimit extends Limit {
    private final boolean value;

    BooleanLimit(boolean value) {
        this.value = value;
    }

    @Override
    Object getValue() {
        return value;
    }

    /**
     * Ok how do you set a boolean limit and violate it?
     *
     * Say it is 'sudo'. You want to express that nobody is allowed to sudo.
     *
     * So you `setLimit(sudo, false)`. I think that is readable enough without further ado.
     *
     * We list that as follows:
     *
     * key  | value
     * sudo | false
     *
     * People can make sense of that.
     *
     * Lastly, we check the limit by going, "if you did supply sudo, it must have be set to false, or it is an error".
     *
     * This achieves that.
     *
     * BUT! What about the opposite case, what does `setLimit("foo", true)` mean?
     *
     * Here I have chosen that it means, "if you did supply foo, it must have be set to true, or it is an error"
     *
     * The alternative was, true is always ranked higher than false. Difficult to think of a useful example of that
     * though. But this is one of those "strong opinion, weakly held" things I guess.
     */
    @Override
    protected boolean isViolatedInternal(Object inputValue) {
        boolean b = (boolean) inputValue;

        return b != value;
    }

    @Override
    String getValueAsString() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    String asErrorMessage(String key, Object value) {
        return formatWithLocale(
            "Configuration parameter '%s' with value '%s' is in violation of it's set limit",
            key,
            value
        );
    }
}
