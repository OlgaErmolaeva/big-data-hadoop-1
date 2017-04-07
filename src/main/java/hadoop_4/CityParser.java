package hadoop_4;

import org.apache.commons.lang.StringUtils;

public class CityParser {
    
    public String parseCity(String line){
        int start = StringUtils.ordinalIndexOf(line, "\t", 7)+1;
        int end = StringUtils.ordinalIndexOf(line, "\t", 8);
        return line.substring(start,end);
    }

}
