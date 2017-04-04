package hadoop_3;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringUtils;

public class UserAgentParser {

    public String parseUserAgent(String line) {
        UserAgent userAgent = new UserAgent(line);
        return userAgent.getBrowser().getGroup().getName();
    }
}
