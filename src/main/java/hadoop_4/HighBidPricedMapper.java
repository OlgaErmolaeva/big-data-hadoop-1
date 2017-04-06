package hadoop_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class HighBidPricedMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private PriceParser priceParser = new PriceParser();
    private CityParser cityParser = new CityParser();
    private CityCachParser cityCachParser = new CityCachParser();

    private Map<String, String> citiesNames;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        citiesNames = cityCachParser.getCitiesNames("/khlkjflgkj;");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        int price = priceParser.parsePrice(line);
        if (price > 250) {
            String cityCode = cityParser.parseCity(line);
            String city = cityCode;
            
            if (citiesNames.containsKey(cityCode)) {
                city = citiesNames.get(cityCode);
            }
            context.write(new Text(city), new IntWritable(price));
        }
    }
}
