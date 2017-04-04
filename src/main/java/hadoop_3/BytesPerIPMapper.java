package hadoop_3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class BytesPerIPMapper extends Mapper<LongWritable, Text, Text, PairWritable> {
    private static final Logger logger = Logger.getLogger(BytesPerIPMapper.class);
    private IpByteParser parser = new IpByteParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.info("mapping key " + key);
        String line = value.toString();

        String ip = parser.parseIP(line);
       long bytes = parser.parseBytes(line);

        context.write(new Text(ip), new PairWritable(1, bytes));
    }
}
