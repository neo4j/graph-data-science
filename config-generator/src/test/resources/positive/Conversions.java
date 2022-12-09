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
package positive;

import org.neo4j.gds.annotation.Configuration;

import java.util.Map;
import java.util.Optional;

public interface Conversions {

    interface BaseConversion {
        static int toIntBase(String input) {
            return Integer.parseInt(input);
        }
    }

    interface OtherConversion {
        static int toIntQual(String input) {
            return Integer.parseInt(input);
        }
    }

    @Configuration("ConversionsConfig")
    interface MyConversion extends BaseConversion {

        @Configuration.ConvertWith(method = "toInt", inverse = "String#valueOf")
        int directMethod();

        @Configuration.ConvertWith(method = "toIntBase", inverse = "String#valueOf")
        int inheritedMethod();

        @Configuration.ConvertWith(method = "positive.Conversions.OtherConversion#toIntQual", inverse = "String#valueOf")
        int qualifiedMethod();

        @Configuration.ConvertWith(method = "add42", inverse = "positive.Conversions.MyConversion#remove42")
        String referenceTypeAsResult();

        @Configuration.ConvertWith(method = "toFoo", inverse = "positive.Conversions.MyConversion#fromFoo")
        Optional<Foo> optional();

        static int toInt(String input) {
            return Integer.parseInt(input);
        }

        static String add42(String input) {
            return input + "42";
        }

        static String remove42(String input) {
            return input.substring(0, input.length() - 2);
        }

        static Foo toFoo(Map<String, Object> parameter) {
            return new Foo() {};
        }
        static Map<String, Object> fromFoo(Foo f) {
            return Map.of();
        }
    }

    interface Foo {}
}
