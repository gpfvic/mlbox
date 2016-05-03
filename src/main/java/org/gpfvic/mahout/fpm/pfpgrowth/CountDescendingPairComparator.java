package org.gpfvic.mahout.fpm.pfpgrowth;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.mahout.common.Pair;

/**
 * Defines an ordering on {@link Pair}s whose second element is a count. The ordering places those with
 * high count first (that is, descending), and for those of equal count, orders by the first element in the
 * pair, ascending. It is used in several places in the FPM code.
 */

public final class CountDescendingPairComparator<A extends Comparable<? super A>,B extends Comparable<? super B>>
  implements Comparator<Pair<A,B>>, Serializable {

  public int compare(Pair<A,B> a, Pair<A,B> b) {
    int ret = b.getSecond().compareTo(a.getSecond());
    if (ret != 0) {
      return ret;
    }
    return a.getFirst().compareTo(b.getFirst());
  }
}
