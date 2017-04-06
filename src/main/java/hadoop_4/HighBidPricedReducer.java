package hadoop_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighBidPricedReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        int currentMax = Integer.MIN_VALUE;

        for (IntWritable value : values) {
            if (value.get() > currentMax) {
                currentMax = value.get();
            }
        }
        context.write(key, new IntWritable(currentMax));
    }
}
