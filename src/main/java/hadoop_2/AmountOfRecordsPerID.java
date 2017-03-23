package hadoop_2;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.*;

public class AmountOfRecordsPerID {

    public static void main(String[] args) {
        Logger logger = Logger.getLogger(AmountOfRecordsPerID.class);
        String uri = "hdfs://192.168.56.101/user/olga/hadoop-2";
        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(uri), configuration);
        } catch (IOException e) {
            logger.error("Exception while fetch File System: " + e);
        }
        Map<String, Long> recordPerID = new HashMap<String, Long>();
        try {
            FileStatus[] statuses = fs.listStatus(new Path(uri));
            for (FileStatus status : statuses) {
                logger.info("Reading file "+ status.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                String line;
                while ((line = br.readLine()) != null) {
                    int startIndex = StringUtils.ordinalIndexOf(line, "\t", 2);
                    int endIndex = StringUtils.ordinalIndexOf(line, "\t", 3);
                    String iPinYouID = line.substring(startIndex + 1, endIndex);
                    if (recordPerID.containsKey(iPinYouID)) {
                        recordPerID.put(iPinYouID, recordPerID.get(iPinYouID) + 1);
                    } else {
                        recordPerID.put(iPinYouID, 1L);
                    }
                }
            }
            logger.info("Map of size " + recordPerID.size() + " with counting results is ready to sort ");
        } catch (IOException e) {
            logger.error("Exception while reading line from  file: " + uri + e);
        }

        ArrayList<Map.Entry<String, Long>> recordsAsList = new ArrayList<Map.Entry<String, Long>>(recordPerID.entrySet());
        Collections.sort(recordsAsList, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        }.reversed());
        try (OutputStream outputStream = fs.create(new Path("hdfs://192.168.56.101/user/olga/bid_result.txt"));
             BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            logger.info("Starting writing results into HDFS: ");
            for (int i = 0; i < 100; i++) {
                bufferedWriter.write(recordsAsList.get(i).getKey() + " - " + recordsAsList.get(i).getValue());
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
        } catch (IOException e) {
            logger.error("Exception while writing to file: " + uri + "/bid_result.txt" + e);
        }
    }
}