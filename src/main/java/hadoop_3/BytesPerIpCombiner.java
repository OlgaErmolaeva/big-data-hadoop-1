package hadoop_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class BytesPerIpCombiner extends Reducer<Text, PairWritable, Text, PairWritable> {

    private static final Logger logger = Logger.getLogger(BytesPerIpCombiner.class);

    @Override
    protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("Combining key "+ key);

        int amountOfIp = 0;
        long totalAmountOfBytes = 0;

        for (PairWritable value : values) {
            amountOfIp += value.getAmountOfIp().get();
            totalAmountOfBytes += value.getTotalByte().get();
        }
        context.write(new Text(key), new PairWritable(amountOfIp, totalAmountOfBytes));
    }
}
