/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package positive;

import javax.annotation.processing.Generated;

import java.util.ArrayList;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ParametersOnlyConfig implements ParametersOnly {

    private int onlyAsParameter;

    public ParametersOnlyConfig(int onlyAsParameter) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.onlyAsParameter = onlyAsParameter;
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        if (!errors.isEmpty()) {
            IllegalArgumentException combinedErrors = new IllegalArgumentException();
            errors.forEach(combinedErrors::addSuppressed);
            throw combinedErrors;
        }
    }

    @Override
    public int onlyAsParameter() {
        return this.onlyAsParameter;
    }
}
