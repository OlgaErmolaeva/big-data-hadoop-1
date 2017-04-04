package hadoop_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BytesPerIpReducerTest {

    @Test
    public void reduceTest() {

        List<PairWritable> value = Arrays.asList(
                new PairWritable(2, 444),
                new PairWritable(3, 999),
                new PairWritable(5, 100));

        new ReduceDriver<Text, PairWritable, Text, Text>()
                .withReducer(new BytesPerIpReducer())
                .withInput(new Text("ip1"), value)
                .withOutput(new Text("ip1"), new Text("154.3, 1543"))
                .runTest();
    }
}
