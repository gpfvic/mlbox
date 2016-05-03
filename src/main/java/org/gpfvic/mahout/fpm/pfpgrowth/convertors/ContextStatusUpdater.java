package org.gpfvic.mahout.fpm.pfpgrowth.convertors;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Updates the Context object of a {@link Reducer} class
 * 
 * @param <IK>
 * @param <IV>
 * @param <K>
 * @param <V>
 */

public class ContextStatusUpdater<IK extends Writable,IV extends Writable,K extends Writable,V extends Writable>
    implements StatusUpdater {
  
  private static final long PERIOD = 10000; // Update every 10 seconds
  
  private final Reducer<IK,IV,K,V>.Context context;
  
  private long time = System.currentTimeMillis();
  
  public ContextStatusUpdater(Reducer<IK,IV,K,V>.Context context) {
    this.context = context;
  }

  public void update(String status) {
    long curTime = System.currentTimeMillis();
    if (curTime - time > PERIOD && context != null) {
      time = curTime;
      context.setStatus("Processing FPTree: " + status);
    }
    
  }
  
}
