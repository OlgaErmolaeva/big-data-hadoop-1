package hadoop_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LongestWordMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueAsString = value.toString();
        String[] words = valueAsString.split(",");
        String longestInLine = words[0];
        for (String word : words) {
            if (word.length() > longestInLine.length()) {
                longestInLine = word;
            }
        }
        NullWritable dummyKey = NullWritable.get();
        context.write(dummyKey, new Text(longestInLine));
    }
}
