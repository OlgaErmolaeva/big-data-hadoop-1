package hadoop_4;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HighBidPricedMapperTest {

    private String input;

    @Before
    public void setUp() throws IOException {
        input = IOUtils.toString(
            this.getClass().getResourceAsStream("/input.txt"), "UTF-8");
    }

    @Test
    public void LongestWordMapperTest() {
        HighBidPricedMapper mapper = new HighBidPricedMapper();
        new MapDriver<LongWritable, Text, Text, IntWritable> ()
            .withMapper(mapper)
            .withInput(new LongWritable(),new Text(input))
            .withOutput(new Text(), new IntWritable())
            .runTest();
    }

    @Test
    public void LongestWordReducerTest() {
        HighBidPricedReducer reducer = new HighBidPricedReducer();
        List<IntWritable> inputList = Arrays.asList();
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
            .withReducer(reducer)
            .withInput(new Text(), inputList)
            .withOutput(new Text(), new IntWritable())
            .runTest();
    }
}