package org.gpfvic.mahout.fpm.pfpgrowth;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Parameters;

/**
 * 
 *  maps all items in a particular transaction like the way it is done in Hadoop
 * WordCount example
 * 
 */

public class ParallelCountingMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
  
  private static final LongWritable ONE = new LongWritable(1);
  
  private Pattern splitter;
  
  @Override
  protected void map(LongWritable offset, Text input, Context context) throws IOException,
                                                                      InterruptedException {
    
    String[] items = splitter.split(input.toString());
    Set<String> uniqueItems = Sets.newHashSet(Arrays.asList(items));
    for (String item : uniqueItems) {
      if (item.trim().isEmpty()) {
        continue;
      }
      context.setStatus("Parallel Counting Mapper: " + item);
      context.write(new Text(item), ONE);
    }
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
    splitter = Pattern.compile(params.get(PFPGrowth.SPLIT_PATTERN, PFPGrowth.SPLITTER.toString()));
  }
}
