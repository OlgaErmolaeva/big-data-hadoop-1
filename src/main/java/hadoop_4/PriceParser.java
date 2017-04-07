package hadoop_4;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class PriceParser {

    private Logger logger = Logger.getLogger(PriceParser.class);

    public int parsePrice(String line) {
        int start = StringUtils.ordinalIndexOf(line, "\t", 20)+1;
        int end = StringUtils.ordinalIndexOf(line, "\t", 21);
        String priceAsString = line.substring(start, end);
        int result = 0;
        try {
            result = Integer.parseInt(priceAsString);
        } catch (NumberFormatException e) {
            logger.info("Cannot parse into long line " + priceAsString);
        }
        return result;
    }
}
