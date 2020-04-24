package dev.suntr.diff;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class Diff<T> implements Serializable {
  private static final long serialVersionUID = -8216244641262752799L;

  public static class StringDiff extends Diff<String> {
    private static final long serialVersionUID = 4533789952845108092L;

    public<C extends Collection> StringDiff diff(final C former, final C later){
      return diff(former, later, Comparator.naturalOrder());
    }
  }

  public static class IntDiff extends Diff<Integer> {
    private static final long serialVersionUID = -8962817947530621627L;

    public<C extends Collection> IntDiff diff(final C former, final C later){
      return diff(former, later, Comparator.naturalOrder());
    }
  }

  public Diff() {
  }

  protected List<T> created = new LinkedList<>();
  protected List<T> deleted = new LinkedList<>();
  protected List<T[]> updated = new LinkedList<>();
  protected List<T[]> remain = new LinkedList<>();

  //新数据属性为null时，不认为是不同
  private boolean ignoreNull = false;
  private boolean ignoreEmpty = false;
  //保留不变的数据
  private boolean keepRemain = false;

  //是否允许相同的key
  private boolean allowDuplicate = false;

  private boolean caseInSensitive = false;

  public <D extends Diff<T>> D ignoreNull(){
    this.ignoreNull = true;
    return (D) this;
  }

  public <D extends Diff<T>> D ignoreEmpty(){
    this.ignoreEmpty = true;
    return (D) this;
  }

  public <D extends Diff<T>> D ignoreBlank(){
    this.ignoreNull = true;
    this.ignoreEmpty = true;
    return (D) this;
  }

  public <D extends Diff<T>> D ignoreCase(){
    this.caseInSensitive = true;
    return (D) this;
  }

  public <D extends Diff<T>> D keepRemain(){
    this.keepRemain = true;
    return (D) this;
  }

  //允许重复的key，并在remain中保留所有重复key的匹配
  public <D extends Diff<T>> D allowDuplicate() {
    this.allowDuplicate = true;
    this.keepRemain = true;
    return (D) this;
  }

  public <C extends Collection<T>, D extends Diff<T>> D diff(C formerCollection, C laterCollection, Comparator<T> comparator){
    return diff(formerCollection, laterCollection, comparator, comparator);
  }

  public<C extends Collection<T>, D extends Diff<T>> D diff(C formerCollection,
                                                                                C laterCollection,
                                                                                Comparator<T> formerKeyComparator,
                                                                                Comparator<T> laterKeyComparator,
                                                                                Comparator<T> compareKeyComparator,
                                                                                BiPredicate<T, T> isEqual){
    if (formerCollection == null || laterCollection == null){throw new IllegalArgumentException("Null Collection");}
    if (formerCollection.size() == 0){this.created.addAll(laterCollection); return (D) this; }
    if (laterCollection.size() == 0){this.deleted.addAll(formerCollection); return (D) this; }
    ListIterator<T> formerIterator =  formerCollection.stream().parallel().sorted(formerKeyComparator).collect(Collectors.toCollection(LinkedList::new)).listIterator();
    ListIterator<T> laterIterator =  laterCollection.stream().parallel().sorted(laterKeyComparator).collect(Collectors.toCollection(LinkedList::new)).listIterator();
    T former = formerIterator.hasNext()? formerIterator.next() : null;
    T later = laterIterator.hasNext()? laterIterator.next() : null;
    while (true) {
      if (former == null && later == null ){
        //以null判断是否结束循环，因此对比的两个list中不能包含null的对象
        break;
      } else if (former == null){
        this.created.add(later);
        later = laterIterator.hasNext()? laterIterator.next(): null;
        continue;
      } else if (later == null) {
        this.deleted.add(former);
        former = formerIterator.hasNext()? formerIterator.next(): null;
        continue;
      }
      int cmp = compareKeyComparator.compare(former, later);
      if (0 == cmp) {
        if (isEqual.test(former, later)){
          if (keepRemain) {
            this.remain.add(arrayIt(former, later));
          }
        } else {
          this.updated.add(arrayIt(former, later));
        }
        if (this.allowDuplicate){
          T formerNext = formerIterator.hasNext()? formerIterator.next(): null;
          T laterNext = laterIterator.hasNext()? laterIterator.next(): null;

          int formerCmp = formerNext==null? 1 : compareKeyComparator.compare(former, formerNext);
          int laterCmp = laterNext==null? 1 : compareKeyComparator.compare(later, laterNext);
          if ( formerCmp == 0 ){
            former = formerNext;
            if (laterNext!=null){
              laterIterator.previous();
            }
          } else if ( laterCmp == 0 ) {
            later = laterNext;
            if (formerNext!=null){
              formerIterator.previous();
            }
          } else {
            former = formerNext;
            later = laterNext;
          }
        } else {
          former = formerIterator.hasNext()? formerIterator.next(): null;
          later = laterIterator.hasNext()? laterIterator.next(): null;
        }
      } else if (cmp > 0) {
        this.created.add(later);
        later = laterIterator.hasNext()? laterIterator.next(): null;
      } else {
        this.deleted.add(former);
        former = formerIterator.hasNext()? formerIterator.next(): null;
      }
    }

    return (D) this;
  }

  public<C extends Collection<T>, D extends Diff<T>> D diff(C formerCollection, C laterCollection, Comparator<T> keyComparator, Comparator<T> propertyComparator){
    return diff(formerCollection, laterCollection, keyComparator, keyComparator, keyComparator, (T a,T b)->propertyComparator.compare(a,b) == 0);
  }

  public<C extends Collection<T>, D extends Diff<T>> D diff(C formerCollection, C laterCollection, Comparator<T> keyComparator, BiPredicate<T, T> propertyComparator){
    return diff(formerCollection, laterCollection, keyComparator, keyComparator, keyComparator, propertyComparator);
  }

  /**
   *
   * @param formerCollection
   * @param laterCollection
   * @param identity 用于唯一标识对象
   * @param keyProperties 用于判断对象是否变更， 当keyProperty等同于identity时，只能判断新增和删除
   * @param <C>
   * @param <M>
   * @param <D>
   * @return
   */

  public<C extends Collection<T>, M extends Comparable<M>, D extends Diff<T>> D diff(final C formerCollection, final C laterCollection, final Function<T, M> identity, final Function<T, ? extends Comparable>... keyProperties){
    Comparator comparator =  Comparator.comparing(identity);
    return (D) diff(formerCollection, laterCollection, comparator, comparator, comparator, (T former, T later)->isEqual(former, later,this.ignoreNull, this.ignoreEmpty, this.caseInSensitive, keyProperties));
  }

  public<C extends Collection<T>, M extends Comparable<M>, D extends Diff<T>> D diff(final C formerCollection, final C laterCollection, final Comparator<T> comparator, final Function<T, ? extends Comparable>... keyProperties){
    return diff(formerCollection, laterCollection,comparator, comparator, comparator, (T former, T later)->isEqual(former, later, this.ignoreNull, this.ignoreEmpty, this.caseInSensitive, keyProperties));
  }

  public<C extends Collection<T>, M extends Comparable<M>, D extends Diff<T>> D diff(final C formerCollection, final Function<T, M> formerIdentityGetter, final C laterCollection, final Function<T, M> laterIdentityGetter, final Function<T, ? extends Comparable>... keyProperties){
    return diff(formerCollection, laterCollection, Comparator.comparing(formerIdentityGetter), Comparator.comparing(laterIdentityGetter),  (T former, T later)->formerIdentityGetter.apply(former).compareTo(laterIdentityGetter.apply(later)), (T former, T later)->isEqual(former, later, this.ignoreNull, this.ignoreEmpty, this.caseInSensitive, keyProperties));
  }

  public static<T> Boolean isEqual(T former, T later, boolean ignoreNull, boolean ignoreEmpty, boolean caseInSensitive, Function<T,?>... propGetters) {
    for (Function<T, ?> getter: propGetters){
      Comparable<Object> latterProp = (Comparable<Object>) getter.apply(later);
      Comparable<Object> formerProp = (Comparable<Object>) getter.apply(former);
      boolean equal = isEqual(formerProp, latterProp, ignoreNull, ignoreEmpty, caseInSensitive);
      if (!equal) {
        return false;
      }
    }
    return true;
  }

  public static boolean isEqual(Object former, Object later, boolean ignoreNull,  boolean ignoreEmpty, boolean caseInSensitive) {
    if (ignoreNull && later == null) {
      return true;
    }
    if (ignoreEmpty && "".equals(later)) {
      return true;
    }
    if (former == null) {
      return later == null;
    }
    if (caseInSensitive && former instanceof String && later instanceof String) {
      return ((String) former).equalsIgnoreCase((String)later);
    }
    return Objects.equals(former, later);
  }

  public<C extends Collection<T>, M extends Comparable<M>, D extends Diff<T>> D diff(final C formerCollection, final C laterCollection, final Function<T, M> identity ){
    return (D) diff(formerCollection, laterCollection, Comparator.comparing(identity), Comparator.comparing(identity));
  }



  public List<T> getCreated() {
    return created;
  }

  public List<T> getDeleted() {
    return deleted;
  }

  public List<T[]> getUpdated() {
    return updated;
  }

  public List<T[]> getRemain() {
    return remain;
  }

  @SuppressWarnings("unchecked")
  private T[] arrayIt(T former, T later){
    T[] u = (T[]) Array.newInstance(former.getClass(), 2);
    u[0] = former;
    u[1] = later;
    return u;
  }
}
