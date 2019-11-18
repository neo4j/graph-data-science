package good;

import org.neo4j.graphalgo.annotation.Configuration;


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

        @Configuration.ConvertWith("toInt")
        int directMethod();

        @Configuration.ConvertWith("toIntBase")
        int inheritedMethod();

        @Configuration.ConvertWith("good.Conversions.OtherConversion#toIntQual")
        int qualifiedMethod();

        static int toInt(String input) {
            return Integer.parseInt(input);
        }
    }
}
