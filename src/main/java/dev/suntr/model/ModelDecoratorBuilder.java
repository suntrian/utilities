package dev.suntr.model;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author suntr
 * @since dmp1.6.0
 *
 *  装饰器模式, 避免对原数据造成影响
 *  通过将普通实体类构建成层级模型，用于模型之间的差异分析，以及回滚，以及版本号计算等等
 *  每层的class类型需一致，暂不支持同一层级下包含不同class类型数据, 支持每层不同class类型，需改写ModelDecorator的function
 *  此方式当模型实体类变更时将不可控，有可能导致存储的树模型和差异数据反序列化失败
 */
@SuppressWarnings("unchecked")
public class ModelDecoratorBuilder {

  private final int initialLevel = 4;

  private final ModelDecorator root = new ModelDecorator();

  private final Map<Integer, List<ModelElement<?>>> elementMap = new HashMap<>(initialLevel);

  private final Map<Integer, BiConsumer<ModelElement<?>, List<ModelElement<?>>>> parentChildrenHandlerMap = new HashMap<>(initialLevel);

  @SafeVarargs
  public final  <T> ModelDecoratorBuilder addData(int level, List<T> data, Function<T, ? extends Comparable<?>> keyGetter, Function<T, ? extends Comparable<?>> parentGetter, Function<T, ? extends Serializable>... propertyGetter){
    LevelFunction<T> levelFunction = new LevelFunction<>();
    levelFunction.setKeyGetter(keyGetter);
    levelFunction.setParentGetter(parentGetter);
    levelFunction.setPropertyGetter(propertyGetter);
    root.getFunctionMap().put(level, (LevelFunction<Object>)levelFunction);
    List<ModelElement<?>> elements = data.stream().map(ModelElement::new).collect(Collectors.toList());
    if (elementMap.containsKey(level)){
      elementMap.get(level).addAll(elements);
    } else {
      elementMap.put(level, elements);
    }
    return this;
  }

  public<E, PK extends Serializable> ModelDecoratorBuilder addEdge(int level, List<E> edges, Function<E, PK> sourceGetter, Function<E, PK> targetGetter){
    return addEdge(level, edges, sourceGetter, targetGetter, null);
  }

  public<E, PK extends Serializable> ModelDecoratorBuilder addEdge(int level, List<E> edges, Function<E, PK> sourceGetter, Function<E, PK> targetGetter, Function<E, ? extends Serializable> extraGetter){
    List<ModelEdge<?>> edgeList;
    if (extraGetter==null){
      edgeList = edges.stream().map(i-> new ModelEdge<>(sourceGetter.apply(i), targetGetter.apply(i))).collect(Collectors.toList());
    } else {
      edgeList = edges.stream().map(i-> new ModelEdge<>(sourceGetter.apply(i), targetGetter.apply(i), extraGetter.apply(i))).collect(Collectors.toList());
    }
    if (root.getEdges().containsKey(level)){
      root.getEdges().get(level).addAll(edgeList);
    } else {
      root.getEdges().put(level, edgeList);
    }
    return this;
  }

  public<P extends Serializable, C extends Serializable> ModelDecoratorBuilder postHandler(Integer parentLevel, BiConsumer<P, List<C>> parentChildrenHandler) {
    this.parentChildrenHandlerMap.put(parentLevel,  (ModelElement<?> parent, List<ModelElement<?>> children)->{
      parentChildrenHandler.accept((P)parent.getElement(), children==null?Collections.emptyList():children.stream().map(ModelElement::getElement).map(x->(C)x).collect(Collectors.toList()));
    });
    return this;
  }


  public ModelDecorator build(){
    List<Integer> levels = elementMap.keySet().stream().sorted().collect(Collectors.toList());
    Map<Object, ModelElement<?>> cache = null;
    for (int i = 0; i < levels.size(); i++){
      final int level = i;
      List<ModelElement<?>> elements = elementMap.getOrDefault(levels.get(level), Collections.emptyList());
      if (i == 0){
        // root层
        elements.forEach(e->e.setLevel(level));
        root.setElements(elements);
      } else {
        for (ModelElement<?> e : elements) {
          e.setLevel(level);
          Object parentKey = root.getFunctionMap().get(levels.get(level)).getParentGetter().apply(e.getElement());
          if (cache == null){
            //normally will never come here
          } else if (cache.containsKey(parentKey)){
            e.setParent(cache.get(parentKey));
            cache.get(parentKey).addChild(e);
          }else {
            //游离的数据处理
            if (root.getDeadElements().containsKey(level)){
              root.getDeadElements().get(level).add(e);
            } else {
              List<ModelElement<?>> list = new LinkedList<>();
              list.add(e);
              root.getDeadElements().put(level, list);
            }
          }
        }
        BiConsumer<ModelElement<?>, List<ModelElement<?>>> parentChildrenPostHandler;
        if ((parentChildrenPostHandler = this.parentChildrenHandlerMap.get(levels.get(i-1)))!=null){
          for (ModelElement<?> value : cache.values()) {
            parentChildrenPostHandler.accept(value, value.getChildren());
          }
        }
      }
      if (i != levels.size()-1){
        cache = elements.stream().collect(Collectors.toMap(e->root.getFunctionMap().get(levels.get(level)).keyGetter.apply(e.getElement()),
                e->e,
                (a, b)->{
                  System.out.println("Duplicate Key Warning A:{" + a.getElement().toString() + "} ,\n B:{" + b.getElement().toString() + "}\n A is used");
                  return a;
        }));
      }
    }
    return root;
  }

  private static String stringfy(Object o){
    if (o==null){ return "";}
    return o.toString();
  }

}
