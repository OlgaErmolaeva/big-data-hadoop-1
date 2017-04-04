package hadoop_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

public class BytesPerIpReducer extends Reducer<Text, PairWritable, Text, Text> {

    private static final Logger logger = Logger.getLogger(BytesPerIPMapper.class);

    @Override
    protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("Reducing key "+ key);
        long amountOfIp = 0;
        long totalAmountOfBytes = 0;

        for (PairWritable value : values) {
            amountOfIp += value.getAmountOfIp().get();
            totalAmountOfBytes += value.getTotalByte().get();
        }
        double averageBytesPerIp = 1.0 * totalAmountOfBytes / amountOfIp;
        DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
        df.setMaximumFractionDigits(1);
        context.write(new Text(key), new Text(df.format(averageBytesPerIp) + ", " + totalAmountOfBytes));
    }
}
