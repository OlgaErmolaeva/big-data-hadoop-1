package hadoop_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LongestWordReducer extends Reducer<NullWritable, Text, IntWritable, Text> {

    private int maxLength;
    private String longestWord;

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        for (Text word : values) {
            if (word.getLength() > maxLength) {
                maxLength = word.getLength();
                longestWord = word.toString();
            }
        }
        context.write(new IntWritable(maxLength), new Text(longestWord));
    }
}
