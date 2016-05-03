package org.gpfvic.mahout.fpm.pfpgrowth;

import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.list.IntArrayList;

/**
 * Iterates over multiple transaction trees to produce a single iterator of transactions
 */

public final class MultiTransactionTreeIterator extends AbstractIterator<IntArrayList> {
  
  private final Iterator<Pair<IntArrayList,Long>> pIterator;
  private IntArrayList current;
  private long currentMaxCount;
  private long currentCount;
  
  public MultiTransactionTreeIterator(Iterator<Pair<IntArrayList,Long>> iterator) {
    this.pIterator = iterator;
  }

  @Override
  protected IntArrayList computeNext() {
    if (currentCount >= currentMaxCount) {
      if (pIterator.hasNext()) {
        Pair<IntArrayList,Long> nextValue = pIterator.next();
        current = nextValue.getFirst();
        currentMaxCount = nextValue.getSecond();
        currentCount = 0;
      } else {
        return endOfData();
      }
    }
    currentCount++;
    return current;
  }
  
}
