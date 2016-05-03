package org.gpfvic.mahout.fpm.pfpgrowth;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  sums up the item count and output the item and the count This can also be
 * used as a local Combiner. A simple summing reducer
 */

public class ParallelCountingReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
  
  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
                                                                                 InterruptedException {
    long sum = 0;
    for (LongWritable value : values) {
      context.setStatus("Parallel Counting Reducer :" + key);
      sum += value.get();
    }
    context.setStatus("Parallel Counting Reducer: " + key + " => " + sum);
    context.write(key, new LongWritable(sum));
    
  }
}
