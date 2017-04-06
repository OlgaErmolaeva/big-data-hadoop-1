package hadoop_4;

import org.apache.commons.lang.StringUtils;

public class CityParser {
    
    public String parseCity(String line){
        int start = StringUtils.ordinalIndexOf(line, "    ", 0);
        int end = StringUtils.ordinalIndexOf(line, "    ", 0);
        return line.substring(start,end);
    }

}
