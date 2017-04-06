package hadoop_4;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CityCachParser {

    private Logger logger = Logger.getLogger(PriceParser.class);
    private Map<String, String> citiesNames = new HashMap<>();

    public Map<String, String> getCitiesNames(String filePath) {

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                addKeyValueFromLine(line);
            }
        } catch (IOException e) {
        }
        return citiesNames;
    }

    private void addKeyValueFromLine(String line) {
        int startOfKey = StringUtils.ordinalIndexOf(line, "    ", 0);
        int endOfKey = StringUtils.ordinalIndexOf(line, "    ", 0);
        String cityCode = "";
        try {
            cityCode = line.substring(startOfKey, endOfKey);
        } catch (Exception e) {
            logger.error("Cannot parse line from cache file, line: " + line + "Exception: " + e);
        }
        if (!cityCode.isEmpty()) {
            int startOfValue = StringUtils.ordinalIndexOf(line, "    ", 0);
            int endOfValue = StringUtils.ordinalIndexOf(line, "    ", 0);
            String cityName = "";
            try {
                cityName = line.substring(startOfValue, endOfValue);
            } catch (Exception e) {
                logger.error("Cannot parse line from cache file, line: " + line + "Exception: " + e);

            }
            if (!cityName.isEmpty()) {
                citiesNames.put(cityCode, cityName);
            }
        }
    }
}