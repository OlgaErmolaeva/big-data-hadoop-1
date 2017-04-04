package hadoop_3;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class IpByteParser {
    private Logger logger = Logger.getLogger(IpByteParser.class);

    public String parseIP(String line) {
        return line.substring(0, line.indexOf(" "));
    }

    public long parseBytes(String line) {
        int start = StringUtils.ordinalIndexOf(line, " ", 9) + 1;
        int end = StringUtils.ordinalIndexOf(line, " ", 10);
        String bytesAsString = line.substring(start, end);
        long totalBytes = 0;
        try {
            return Long.parseLong(bytesAsString);
        } catch (NumberFormatException e) {
            logger.info("Exception while parsing bytes: "+ bytesAsString + ". Set byte amount to 0");
        }
        return totalBytes;
    }
}


