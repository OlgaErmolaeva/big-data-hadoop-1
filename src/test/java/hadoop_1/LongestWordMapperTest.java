package hadoop_1;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class LongestWordMapperTest {

    private MapDriver<LongWritable, Text, NullWritable, Text> mapDriver;
    private ReduceDriver<NullWritable, Text, IntWritable, Text> reduceDriver;
    private String input;

    @Before
    public void setUp() throws IOException {
        input = IOUtils.toString(
            this.getClass().getResourceAsStream("/input.txt"), "UTF-8");
    }

    @Test
    public void LongestWordMapperTest() {
        LongestWordMapper mapper = new LongestWordMapper();
        new MapDriver<LongWritable, Text, NullWritable, Text>()
            .withMapper(mapper)
            .withInput(new LongWritable(),new Text(input))
            .withOutput(NullWritable.get(), new Text("asdfghjkl"))
            .runTest();
    }

    @Test
    public void LongestWordReducerTest() {
        LongestWordReducer reducer = new LongestWordReducer();
        List<Text> inputList = Arrays.asList(new Text("qwert"),new Text("asdfghjkl"));
        new ReduceDriver<NullWritable, Text, IntWritable, Text>()
            .withReducer(reducer)
            .withInput(NullWritable.get(),inputList)
            .withOutput(new IntWritable(9), new Text("asdfghjkl"))
            .runTest();
    }
}