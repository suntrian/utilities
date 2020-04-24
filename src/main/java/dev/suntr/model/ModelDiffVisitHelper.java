package dev.suntr.model;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dev.suntr.model.ModelDiffWrapper.State.*;

/**
 * @author suntr
 * @version dmp1.6.0
 *
 * 用于遍历{@link ModelDiffWrapper}对象
 */
@SuppressWarnings("unchecked")
public class ModelDiffVisitHelper {
  public enum Order{
    LRD("后序"),
    DLR("前序"),
    //非二叉树，无所谓中序
    //LDR("中序"),
    LEVEL("层次");

    private String desc;

    Order(String desc) {
      this.desc = desc;
    }
  }

  /**
   *  暂时没做边的reduce
   */
  public static class VisitResult {
    private int level;

    //实际上数据库变更执行的结果
    private Object createdReduce;
    private Object updatedReduce;
    private Object deletedReduce;
    private Object remainReduce;

    //对比上存在变化的数量
    private int createdCompareCount;
    private int updatedCompareCount;
    private int deletedCompareCount;
    private int remainCompareCount;

    //数据库执行变更失败的数量
    private int createdFailCount;
    private int updatedFailCount;
    private int deletedFailCount;

    public VisitResult(int level, Object createdReduce, Object updatedReduce, Object deletedReduce, Object remainReduce,
                       int createdCompareCount, int updatedCompareCount, int deletedCompareCount, int remainCompareCount) {
      this.level = level;
      this.createdReduce = createdReduce;
      this.updatedReduce = updatedReduce;
      this.deletedReduce = deletedReduce;
      this.remainReduce = remainReduce;
      this.createdCompareCount = createdCompareCount;
      this.updatedCompareCount = updatedCompareCount;
      this.deletedCompareCount = deletedCompareCount;
      this.remainCompareCount = remainCompareCount;
    }

    public int getLevel() {
      return level;
    }

    public void setLevel(int level) {
      this.level = level;
    }

    public Object getCreatedReduce() {
      return createdReduce;
    }

    public void setCreatedReduce(Object createdReduce) {
      this.createdReduce = createdReduce;
    }

    public Object getUpdatedReduce() {
      return updatedReduce;
    }

    public void setUpdatedReduce(Object updatedReduce) {
      this.updatedReduce = updatedReduce;
    }

    public Object getDeletedReduce() {
      return deletedReduce;
    }

    public void setDeletedReduce(Object deletedReduce) {
      this.deletedReduce = deletedReduce;
    }

    public Object getRemainReduce() {
      return remainReduce;
    }

    public void setRemainReduce(Object remainReduce) {
      this.remainReduce = remainReduce;
    }

    public int getCreatedCompareCount() {
      return createdCompareCount;
    }

    public void setCreatedCompareCount(int createdCompareCount) {
      this.createdCompareCount = createdCompareCount;
    }

    public int getUpdatedCompareCount() {
      return updatedCompareCount;
    }

    public void setUpdatedCompareCount(int updatedCompareCount) {
      this.updatedCompareCount = updatedCompareCount;
    }

    public int getDeletedCompareCount() {
      return deletedCompareCount;
    }

    public void setDeletedCompareCount(int deletedCompareCount) {
      this.deletedCompareCount = deletedCompareCount;
    }

    public int getRemainCompareCount() {
      return remainCompareCount;
    }

    public void setRemainCompareCount(int remainCompareCount) {
      this.remainCompareCount = remainCompareCount;
    }

    public int getCreatedFailCount() {
      return createdFailCount;
    }

    public void setCreatedFailCount(int createdFailCount) {
      this.createdFailCount = createdFailCount;
    }

    public int getUpdatedFailCount() {
      return updatedFailCount;
    }

    public void setUpdatedFailCount(int updatedFailCount) {
      this.updatedFailCount = updatedFailCount;
    }

    public int getDeletedFailCount() {
      return deletedFailCount;
    }

    public void setDeletedFailCount(int deletedFailCount) {
      this.deletedFailCount = deletedFailCount;
    }
  }

  private int initialSize = 4;

  private Order order = Order.DLR;

  private final Map<Integer, Function<Object, Object>> createdElementFunction = new HashMap<>(initialSize);
  private final Map<Integer, Function<Object, Object>> deletedElementFunction = new HashMap<>(initialSize);
  private final Map<Integer, BiFunction<Object, Object, Object>> updatedElementFunction = new HashMap<>(initialSize);
  private final Map<Integer, BiFunction<Object, Object, Object>> remainElementFunction = new HashMap<>(initialSize);

  private final Map<Integer, Function<Collection<Object>, Object>> createdElementBatchFunction = new HashMap<>(initialSize);
  private final Map<Integer, Function<Collection<Object>, Object>> deletedElementBatchFunction = new HashMap<>(initialSize);
  private final Map<Integer, BiFunction<Collection<Object>, Collection<Object>, Object>> updatedElementBatchFunction = new HashMap<>(initialSize);
  private final Map<Integer, BiFunction<Collection<Object>, Collection<Object>, Object>> remainElementBatchFunction = new HashMap<>(initialSize);

  private final Map<Integer, TriFunction<Object, Object, Object, Object>> createdEdgeFunction = new HashMap<>(initialSize);
  private final Map<Integer, TriFunction<Object, Object, Object, Object>> deletedEdgeFunction = new HashMap<>(initialSize);

  private final Map<Integer, BiConsumer<ModelDiffElement<?>, ModelDiffElement<?>>> preCreatedParentConsumer = new HashMap<>(initialSize);
  private final Map<Integer, BiConsumer<ModelDiffElement<?>, ModelDiffElement<?>>> preDeletedParentConsumer = new HashMap<>(initialSize);
  private final Map<Integer, BiConsumer<ModelDiffElement<?>, ModelDiffElement<?>>> preUpdatedParentConsumer = new HashMap<>(initialSize);
  private final Map<Integer, BiConsumer<ModelDiffElement<?>, ModelDiffElement<?>>> preRemainParentConsumer = new HashMap<>(initialSize);

  private final Map<Integer, BiConsumer<ModelDiffElement<?>, ModelDiffElement<?>>> postCreatedParentConsumer = new HashMap<>(initialSize);
  private final Map<Integer, BiConsumer<ModelDiffElement<?>, ModelDiffElement<?>>> postDeletedParentConsumer = new HashMap<>(initialSize);
  private final Map<Integer, BiConsumer<ModelDiffElement<?>, ModelDiffElement<?>>> postUpdatedParentConsumer = new HashMap<>(initialSize);
  private final Map<Integer, BiConsumer<ModelDiffElement<?>, ModelDiffElement<?>>> postRemainParentConsumer = new HashMap<>(initialSize);

  private final Map<Integer, BiFunction<Object, Object, Object>> createdReduce = new HashMap<>(initialSize);
  private final Map<Integer, BiFunction<Object, Object, Object>> deletedReduce = new HashMap<>(initialSize);
  private final Map<Integer, BiFunction<Object, Object, Object>> updatedReduce = new HashMap<>(initialSize);
  private final Map<Integer, BiFunction<Object, Object, Object>> remainReduce =  new HashMap<>(initialSize);
  private final Map<Integer, Object> createdDefaultReduceValue = new HashMap<>(initialSize);
  private final Map<Integer, Object> updatedDefaultReduceValue = new HashMap<>(initialSize);
  private final Map<Integer, Object> deletedDefaultReduceValue = new HashMap<>(initialSize);
  private final Map<Integer, Object> remainDefaultReduceValue = new HashMap<>(initialSize);

  private final Map<Integer, BiFunction<Object, Object, Object>> createdEdgeReduce = new HashMap<>(initialSize);
  private final Map<Integer, BiFunction<Object, Object, Object>> deletedEdgeReduce = new HashMap<>(initialSize);

  private final Map<Integer, VisitResult> reduceResult = new HashMap<>(initialSize);

  private List<Integer> elementLevels0Based;
  private List<Integer> edgeLevels0Based;

  public Map<Integer, VisitResult> visit(ModelDiffWrapper wrapper){
    if (checkElementHandler()){
      elementLevels0Based = wrapper.getFunctionMap().keySet().stream().sorted().collect(Collectors.toList());
      VisitResult rootResult = visitElements(wrapper.getElementDiff(), 0);
      if (rootResult!=null){
        reduceResult.put(elementLevels0Based.get(0), rootResult);
      }
    }
    if (checkEdgeHandler()){
      edgeLevels0Based = checkEdgeLevel();
      visitEdges(wrapper.getEdgeDiff());
    }
    List<Integer> levels = Stream.of(createdDefaultReduceValue.keySet(), updatedDefaultReduceValue.keySet(), deletedDefaultReduceValue.keySet(), remainDefaultReduceValue.keySet())
            .flatMap(Collection::stream).distinct().sorted().collect(Collectors.toList());
    for (Integer lv: levels){
      reduceResult.putIfAbsent(lv, new VisitResult(lv,
              createdDefaultReduceValue.getOrDefault(lv, null),
              updatedDefaultReduceValue.getOrDefault(lv,null),
              deletedDefaultReduceValue.getOrDefault(lv,null),
              remainDefaultReduceValue.getOrDefault(lv,null),
              0, 0, 0, 0));
    }
    return reduceResult;
  }

  //前序遍历树
  private VisitResult visitElements(List<ModelDiffElement<?>> diffElements, int level){
    if (diffElements == null || diffElements.size() == 0){
      return null;
    }
    List<Object> createdList = new LinkedList<>(),
            deletedList = new LinkedList<>(),
            updatedOriginalList = new LinkedList<>(),
            updatedCurrentList = new LinkedList<>(),
            remainOriginalList = new LinkedList<>(),
            remainCurrentList = new LinkedList<>();

    List<Object> createdResult = new LinkedList<>(),
            updatedResult = new LinkedList<>(),
            deletedResult = new LinkedList<>(),
            remainResult = new LinkedList<>();

    List<VisitResult> childrenVisitResult = new LinkedList<>();

    for (ModelDiffElement<?> e: diffElements){
      if (Order.LRD.equals(order)){
        childrenVisitResult.add(visitElements(e.getChildren(), level+1));
      }
      switch (e.getState()){
        case CREATED:
          if (elementLevels0Based.size()>0 && this.preCreatedParentConsumer.get(elementLevels0Based.get(level))!=null){
            this.preCreatedParentConsumer.get(elementLevels0Based.get(level)).accept(e.getParent(), e);
          }
          if (elementLevels0Based.size()>0 && this.createdElementFunction.get(elementLevels0Based.get(level))!=null){
            createdResult.add(this.createdElementFunction.get(elementLevels0Based.get(level)).apply(e.getCurrent()));
          }
          createdList.add(e.getCurrent());
          if (elementLevels0Based.size()>0 && this.postCreatedParentConsumer.get(elementLevels0Based.get(level))!=null){
            this.postCreatedParentConsumer.get(elementLevels0Based.get(level)).accept(e.getParent(), e);
          }
          break;
        case DELETED:
          if (elementLevels0Based.size()>level && this.preDeletedParentConsumer.get(elementLevels0Based.get(level))!=null){
            this.preDeletedParentConsumer.get(elementLevels0Based.get(level)).accept(e.getParent(), e);
          }
          if (elementLevels0Based.size()>level && this.deletedElementFunction.get(elementLevels0Based.get(level))!=null){
            deletedResult.add(this.deletedElementFunction.get(elementLevels0Based.get(level)).apply(e.getOriginal()));
          }
          deletedList.add(e.getOriginal());
          if (elementLevels0Based.size()>0 && this.postDeletedParentConsumer.get(elementLevels0Based.get(level))!=null){
            this.postDeletedParentConsumer.get(elementLevels0Based.get(level)).accept(e.getParent(), e);
          }
          break;
        case REMAIN:
          if (elementLevels0Based.size()>level && this.preRemainParentConsumer.get(elementLevels0Based.get(level))!=null){
            this.preRemainParentConsumer.get(elementLevels0Based.get(level)).accept(e.getParent(), e);
          }
          if (elementLevels0Based.size()>level && this.remainElementFunction.get(elementLevels0Based.get(level))!=null){
            remainResult.add(this.remainElementFunction.get(elementLevels0Based.get(level)).apply(e.getOriginal(), e.getCurrent()));
          }
          remainOriginalList.add(e.getOriginal());
          remainCurrentList.add(e.getCurrent());
          if (elementLevels0Based.size()>0 && this.postRemainParentConsumer.get(elementLevels0Based.get(level))!=null){
            this.postRemainParentConsumer.get(elementLevels0Based.get(level)).accept(e.getParent(), e);
          }
          break;
        case UPDATED:
          if (elementLevels0Based.size()>level && this.preUpdatedParentConsumer.get(elementLevels0Based.get(level))!=null){
            this.preUpdatedParentConsumer.get(elementLevels0Based.get(level)).accept(e.getParent(), e);
          }
          if (elementLevels0Based.size()>level && this.updatedElementFunction.get(elementLevels0Based.get(level))!=null){
            updatedResult.add(this.updatedElementFunction.get(elementLevels0Based.get(level)).apply(e.getOriginal(), e.getCurrent()));
          }
          updatedOriginalList.add(e.getOriginal());
          updatedCurrentList.add(e.getCurrent());
          if (elementLevels0Based.size()>0 && this.postUpdatedParentConsumer.get(elementLevels0Based.get(level))!=null){
            this.postUpdatedParentConsumer.get(elementLevels0Based.get(level)).accept(e.getParent(), e);
          }
          break;
      }
    };
    if (elementLevels0Based.size()>level && this.createdElementBatchFunction.get(elementLevels0Based.get(level))!=null && createdList.size()>0){
      createdResult.add(this.createdElementBatchFunction.get(elementLevels0Based.get(level)).apply(createdList));
    }
    if (elementLevels0Based.size()>level && this.deletedElementBatchFunction.get(elementLevels0Based.get(level))!=null && deletedList.size()>0){
      deletedResult.add(this.deletedElementBatchFunction.get(elementLevels0Based.get(level)).apply(deletedList));
    }
    if (elementLevels0Based.size()>level && this.updatedElementBatchFunction.get(elementLevels0Based.get(level))!=null && updatedCurrentList.size()>0){
      updatedResult.add(this.updatedElementBatchFunction.get(elementLevels0Based.get(level)).apply(updatedOriginalList, updatedCurrentList));
    }
    if (elementLevels0Based.size()>level && this.remainElementBatchFunction.get(elementLevels0Based.get(level))!=null && remainCurrentList.size()>0){
      remainResult.add(this.remainElementBatchFunction.get(elementLevels0Based.get(level)).apply(remainOriginalList, remainCurrentList));
    }
    if (Order.DLR.equals(order)){
      for (ModelDiffElement<?> i : diffElements) {
        childrenVisitResult.add(visitElements(i.getChildren(), level +1 ));
      }
    } else if (Order.LEVEL.equals(order)){
      childrenVisitResult.add(
          visitElements(diffElements.stream()
              .map(ModelDiffElement::getChildren)
              .filter(i->i!=null&&i.size()>0)
              .flatMap(Collection::stream)
              .collect(Collectors.toList()),
                  level + 1)
      );
    }
    childrenVisitResult.stream().filter(Objects::nonNull).reduce((VisitResult a, VisitResult b) -> mergeReduceResult(a, b, level)).ifPresent(childResult -> {
      if(!this.reduceResult.containsKey(elementLevels0Based.get(level+1))){
        this.reduceResult.put(elementLevels0Based.get(level + 1), childResult);
      } else {
        VisitResult formerResult = reduceResult.get(elementLevels0Based.get(level+1));
        mergeReduceResult(formerResult, childResult, level);
      }
    });
    return new VisitResult(elementLevels0Based.get(level),
            createdReduce.containsKey(elementLevels0Based.get(level))?createdResult.stream().reduce((a, b)->createdReduce.get(elementLevels0Based.get(level)).apply(a,b)).orElse(createdDefaultReduceValue.get(elementLevels0Based.get(level))):null,
            updatedReduce.containsKey(elementLevels0Based.get(level))?updatedResult.stream().reduce((a, b)->updatedReduce.get(elementLevels0Based.get(level)).apply(a,b)).orElse(updatedDefaultReduceValue.get(elementLevels0Based.get(level))):null,
            deletedReduce.containsKey(elementLevels0Based.get(level))?deletedResult.stream().reduce((a, b)->deletedReduce.get(elementLevels0Based.get(level)).apply(a,b)).orElse(deletedDefaultReduceValue.get(elementLevels0Based.get(level))):null,
            remainReduce.containsKey(elementLevels0Based.get(level))?remainResult.stream().reduce((a, b)->remainReduce.get(elementLevels0Based.get(level)).apply(a,b)).orElse(remainDefaultReduceValue.get(elementLevels0Based.get(level))):null,
            createdList.size(),
            updatedOriginalList.size(),
            deletedList.size(),
            remainOriginalList.size()
    );
  }

  private void visitEdges(List<ModelDiffEdge> diffEdges){
    for (ModelDiffEdge diffEdge: diffEdges) {
      if (this.createdEdgeFunction.get(edgeLevels0Based.get(diffEdge.getLevel())) != null) {
        for (ModelEdge edge : diffEdge.getCreated()) {
          this.createdEdgeFunction.get(edgeLevels0Based.get(diffEdge.getLevel())).apply(edge.getSource(), edge.getTarget(), edge.getExtra());
        }
      }
      if (this.deletedEdgeFunction.get(edgeLevels0Based.get(diffEdge.getLevel())) != null){
        for (ModelEdge edge : diffEdge.getDeleted()) {
          this.createdEdgeFunction.get(edgeLevels0Based.get(diffEdge.getLevel())).apply(edge.getSource(), edge.getTarget(), edge.getExtra());
        }
      }
    }
  }

  private VisitResult mergeReduceResult(VisitResult a, VisitResult b, int level){
    a.setCreatedReduce(createdReduce.containsKey(elementLevels0Based.get(level + 1)) ? createdReduce.get(elementLevels0Based.get(level+1)).apply(a.getCreatedReduce(), b.getCreatedReduce()) : null );
    a.setUpdatedReduce(updatedReduce.containsKey(elementLevels0Based.get(level + 1)) ? updatedReduce.get(elementLevels0Based.get(level+1)).apply(a.getUpdatedReduce(), b.getUpdatedReduce()) : null );
    a.setDeletedReduce(deletedReduce.containsKey(elementLevels0Based.get(level + 1)) ? deletedReduce.get(elementLevels0Based.get(level+1)).apply(a.getDeletedReduce(), b.getDeletedReduce()) : null );
    a.setRemainReduce(remainReduce.containsKey(elementLevels0Based.get(level + 1)) ?    remainReduce.get(elementLevels0Based.get(level+1)).apply(a.getRemainReduce(), b.getRemainReduce()) : null );
    a.setCreatedCompareCount(a.getCreatedCompareCount() + b.getCreatedCompareCount());
    a.setDeletedCompareCount(a.getDeletedCompareCount() + b.getDeletedCompareCount());
    a.setUpdatedCompareCount(a.getUpdatedCompareCount() + b.getUpdatedCompareCount());
    a.setRemainCompareCount(a.getRemainCompareCount() + b.getRemainCompareCount());
    return a;
  }

  private boolean checkElementHandler(){
    return this.createdElementFunction.size()
            +this.deletedElementFunction.size()
            +this.updatedElementFunction.size()
            +this.remainElementFunction.size()
            +this.createdElementBatchFunction.size()
            +this.deletedElementBatchFunction.size()
            +this.updatedElementBatchFunction.size()
            +this.remainElementBatchFunction.size()
            > 0;
  }

  private boolean checkEdgeHandler(){
    return this.createdEdgeFunction.size() + this.deletedEdgeFunction.size() > 0;
  }

  private List<Integer> checkEdgeLevel(){
    Set<Integer> list = new HashSet<>(this.createdEdgeFunction.size() + this.deletedEdgeFunction.size());
    list.addAll(this.createdEdgeFunction.keySet());
    list.addAll(this.deletedEdgeFunction.keySet());
    return list.stream().sorted().collect(Collectors.toList());
  }

  public ModelDiffVisitHelper setVisitOrder(Order order){
    this.order = order;
    return this;
  }

  public<T extends Serializable, R> ModelDiffVisitHelper setCreatedHandler(final int level, Function<T, R> handler){
    setElementHandler(level, CREATED, handler);
    return this;
  }

  public<T extends Serializable, R> ModelDiffVisitHelper setDeletedHandler(final int level, Function<T, R> handler){
    setElementHandler(level, ModelDiffWrapper.State.DELETED, handler);
    return this;
  }

  public<T extends Serializable, R> ModelDiffVisitHelper setUpdatedHandler(final int level, BiFunction<T, T, R> handler){
    setElementHandler(level, ModelDiffWrapper.State.UPDATED, handler);
    return this;
  }

  public<T extends Serializable, R> ModelDiffVisitHelper setUpdatedHandler(final int level, Function<T, R> handler){
    setElementHandler(level, ModelDiffWrapper.State.UPDATED, handler);
    return this;
  }

  public<T extends Serializable, R> ModelDiffVisitHelper setRemainedHandler(final int level, BiFunction<T, T, R> handler){
    setElementHandler(level, ModelDiffWrapper.State.REMAIN, handler);
    return this;
  }

  public<T extends Serializable, R> ModelDiffVisitHelper setRemainedHandler(final int level, Function<T, R> handler){
    setElementHandler(level, ModelDiffWrapper.State.REMAIN, handler);
    return this;
  }

  public<U extends Serializable, V extends Serializable> ModelDiffVisitHelper setPreCreatedParentConsumer(final int level, BiConsumer<ModelDiffElement<U>, ModelDiffElement<V>> consumer){
    this.preCreatedParentConsumer.put(level, (ModelDiffElement<?> parent, ModelDiffElement<?> child)
            -> consumer.accept((ModelDiffElement<U>)parent, (ModelDiffElement<V>) child));
    return this;
  }
  public<U extends Serializable, V extends Serializable> ModelDiffVisitHelper setPreDeletedParentConsumer(final int level, BiConsumer<ModelDiffElement<U>, ModelDiffElement<V>> consumer){
    this.preDeletedParentConsumer.put(level, (ModelDiffElement<?> parent, ModelDiffElement<?> child)
            -> consumer.accept((ModelDiffElement<U>)parent, (ModelDiffElement<V>) child));
    return this;
  }
  public<U extends Serializable, V extends Serializable> ModelDiffVisitHelper setPreUpdatedParentConsumer(final int level, BiConsumer<ModelDiffElement<U>, ModelDiffElement<V>> consumer){
    this.preUpdatedParentConsumer.put(level, (ModelDiffElement<?> parent, ModelDiffElement<?> child)
            -> consumer.accept((ModelDiffElement<U>)parent, (ModelDiffElement<V>) child));
    return this;
  }
  public<U extends Serializable, V extends Serializable> ModelDiffVisitHelper setPreRemainParentConsumer(final int level, BiConsumer<ModelDiffElement<U>, ModelDiffElement<V>> consumer){
    this.preRemainParentConsumer.put(level, (ModelDiffElement<?> parent, ModelDiffElement<?> child)
            -> consumer.accept((ModelDiffElement<U>)parent, (ModelDiffElement<V>) child));
    return this;
  }

  public<U extends Serializable, V extends Serializable> ModelDiffVisitHelper setPostCreatedParentConsumer(final int level, BiConsumer<ModelDiffElement<U>, ModelDiffElement<V>> consumer){
    this.postCreatedParentConsumer.put(level, (ModelDiffElement<?> parent, ModelDiffElement<?> child)
            -> consumer.accept((ModelDiffElement<U>)parent, (ModelDiffElement<V>) child));
    return this;
  }
  public<U extends Serializable, V extends Serializable> ModelDiffVisitHelper setPostDeletedParentConsumer(final int level, BiConsumer<ModelDiffElement<U>, ModelDiffElement<V>> consumer){
    this.postDeletedParentConsumer.put(level, (ModelDiffElement<?> parent, ModelDiffElement<?> child)
            -> consumer.accept((ModelDiffElement<U>)parent, (ModelDiffElement<V>) child));
    return this;
  }
  public<U extends Serializable, V extends Serializable> ModelDiffVisitHelper setPostUpdatedParentConsumer(final int level, BiConsumer<ModelDiffElement<U>, ModelDiffElement<V>> consumer){
    this.postUpdatedParentConsumer.put(level, (ModelDiffElement<?> parent, ModelDiffElement<?> child)
            -> consumer.accept((ModelDiffElement<U>)parent, (ModelDiffElement<V>) child));
    return this;
  }
  public<U extends Serializable, V extends Serializable> ModelDiffVisitHelper setPostRemainParentConsumer(final int level, BiConsumer<ModelDiffElement<U>, ModelDiffElement<V>> consumer){
    this.postRemainParentConsumer.put(level, (ModelDiffElement<?> parent, ModelDiffElement<?> child)
            -> consumer.accept((ModelDiffElement<U>)parent, (ModelDiffElement<V>) child));
    return this;
  }

  public<T extends Serializable, C extends Collection<T>, R> ModelDiffVisitHelper setBatchCreatedHandler(final int level, Function<C, R> handler){
    this.createdElementBatchFunction.put(level, (Function<Collection<Object>, Object>) handler);
    return this;
  }

  public<T extends Serializable, C extends Collection<T>, R> ModelDiffVisitHelper setBatchDeletedHandler(final int level, Function<C, R> handler){
    this.deletedElementBatchFunction.put(level, (Function<Collection<Object>, Object>) handler);
    return this;
  }

  public<T extends Serializable, C extends Collection<T>, R> ModelDiffVisitHelper setBatchUpdatedHandler(final int level, BiFunction<C, C, R> handler){
    this.updatedElementBatchFunction.put(level, (BiFunction<Collection<Object>, Collection<Object>, Object>) handler);
    return this;
  }

  public<T extends Serializable, C extends Collection<T>, R> ModelDiffVisitHelper setBatchRemainHandler(final int level, BiFunction<C, C, R> handler){
    this.remainElementBatchFunction.put(level, (BiFunction<Collection<Object>, Collection<Object>, Object>) handler);
    return this;
  }

  public<T> ModelDiffVisitHelper setCreatedReduce(final  int level, BiFunction<T, T, T> reduce, T defaultValue){
    setReduce(level, CREATED, reduce);
    this.createdDefaultReduceValue.put(level, defaultValue);
    return this;
  }

  public<T> ModelDiffVisitHelper setUpdatedReduce(final  int level, BiFunction<T, T, T> reduce, T defaultValue){
    setReduce(level, ModelDiffWrapper.State.UPDATED, reduce);
    this.updatedDefaultReduceValue.put(level, defaultValue);
    return this;
  }

  public<T> ModelDiffVisitHelper setDeletedReduce(final  int level, BiFunction<T, T, T> reduce, T defaultValue){
    setReduce(level, ModelDiffWrapper.State.DELETED, reduce);
    this.deletedDefaultReduceValue.put(level, defaultValue);
    return this;
  }

  public<T> ModelDiffVisitHelper setRemainReduce(final  int level, BiFunction<T, T, T> reduce, T defaultValue){
    setReduce(level, ModelDiffWrapper.State.REMAIN, reduce);
    this.remainDefaultReduceValue.put(level, defaultValue);
    return this;
  }


  private <T> void setElementHandler(final int level, ModelDiffWrapper.State state, Function<T, ?> handler){
    switch (state){
      case CREATED:
        this.createdElementFunction.put(level, (Function<Object, Object>) handler);
        break;
      case DELETED:
        this.deletedElementFunction.put(level, (Function<Object, Object>) handler);
        break;
      case REMAIN:
        //只处理新的数据，即第二个数据
        this.remainElementFunction.put(level, (Object a, Object b)-> handler.apply((T)b) );
        break;
      case UPDATED:
        //只处理新的数据，即第二个数据
        this.updatedElementFunction.put(level, (Object a, Object b)-> handler.apply((T) b));
        break;
      default:
    }
  }

  private<T, R> void setElementHandler(final int level, ModelDiffWrapper.State state, BiFunction<T, T, R> handler){
    switch (state) {
      case CREATED:
        this.createdElementFunction.put(level, (Object s)->handler.apply(null, (T) s));
        break;
      case DELETED:
        this.deletedElementFunction.put(level, (Object s)->handler.apply((T) s, null));
        break;
      case UPDATED:
        this.updatedElementFunction.put(level, (BiFunction<Object, Object, Object>) handler);
        break;
      case REMAIN:
        this.remainElementFunction.put(level, (BiFunction<Object, Object, Object>) handler);
        break;
    }
  }

  private void setEdgeHandler(final int level, ModelDiffWrapper.State state, TriFunction<Object, Object, Object, Object> handler){
    switch (state){
      case CREATED:
        this.createdEdgeFunction.put(level, handler);
        break;
      case DELETED:
        this.deletedEdgeFunction.put(level, handler);
        break;
      case UPDATED:
      case REMAIN:
        default:
          //do nothing;
    }
  }

  private<T> void setReduce(final int level, ModelDiffWrapper.State state, BiFunction<T, T, T> reduce){
    switch (state){
      case CREATED:
        this.createdReduce.put(level, (BiFunction<Object, Object, Object>) reduce);
        break;
      case UPDATED:
        this.updatedReduce.put(level, (BiFunction<Object, Object, Object>) reduce);
        break;
      case REMAIN:
        this.remainReduce.put(level, (BiFunction<Object, Object, Object>) reduce);
        break;
      case DELETED:
        this.deletedReduce.put(level, (BiFunction<Object, Object, Object>) reduce);
        break;
      default:
    }
  }

  public static void removeCreateOrDeleteElement(ModelDiffWrapper diffWrapper, ModelDiffWrapper.State state) {
    removeCreateOrDeleteElement(diffWrapper.getElementDiff(), state);
  }

  private static void removeCreateOrDeleteElement(List<ModelDiffElement<?>> diffElements, ModelDiffWrapper.State state) {
    if ( !DELETED .equals(state) && !CREATED.equals(state)){
      throw new RuntimeException("state must be delete or create!");
    }
    if (diffElements == null || diffElements.size() == 0) return;
    for (Iterator<ModelDiffElement<?>> iterator = diffElements.iterator(); iterator.hasNext(); ) {
      ModelDiffElement<?> element = iterator.next();
      if (element.getState() == state){
        iterator.remove();
        continue;
      } else {
        removeCreateOrDeleteElement(element.getChildren(), state);
      }
      if (REMAIN == element.getState()){
        if (element.getChildren() == null || element.getChildren().size() == 0){
          iterator.remove();
        }
      }
    }
  }

  public static void removeRemainElement(ModelDiffWrapper diffWrapper){
    removeRemainElement(diffWrapper.getElementDiff());
  }

  private static void removeRemainElement(List<ModelDiffElement<?>> diffElements){
    if (diffElements == null || diffElements.isEmpty()) {return;}
    Iterator<ModelDiffElement<?>> iterator = diffElements.iterator();
    while (iterator.hasNext()){
      ModelDiffElement<?> element = iterator.next();
      removeRemainElement(element.getChildren());
      if (element.getState() == REMAIN && (element.getChildren() == null || element.getChildren().isEmpty())){
        iterator.remove();
      }
    }
  }

}
