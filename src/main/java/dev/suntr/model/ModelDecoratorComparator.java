package dev.suntr.model;

import dev.suntr.diff.Diff;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class ModelDecoratorComparator {

    private static final int INITIAL_SIZE = 6;
    public final static String split = "|";
    private final Map<Integer, Function<ModelElement, Comparable>> formerKeyMap = new HashMap<>(INITIAL_SIZE);
    private final Map<Integer, Function<ModelElement, Comparable>> latterKeyMap = new HashMap<>(INITIAL_SIZE);
    private final Map<Integer, Comparator<?>> keyComparatorMap = new HashMap<>(INITIAL_SIZE);
    private final Map<Integer, BiPredicate<?, ?>> propComparatorMap = new HashMap<>(INITIAL_SIZE);

    private List<Integer> functionLevel0Based;

    private boolean ignoreNull = false;
    private boolean ignoreEmpty = false;
    private boolean caseInSensitive = false;

    public ModelDecoratorComparator ignoreNull(){
        this.ignoreNull = true;
        return this;
    }

    public ModelDecoratorComparator ignoreEmpty(){
        this.ignoreEmpty = true;
        return this;
    }

    public ModelDecoratorComparator ignoreBlank(){
        this.ignoreNull = true;
        this.ignoreEmpty = true;
        return this;
    }

    public ModelDecoratorComparator caseInSensitive(){
        this.caseInSensitive = true;
        return this;
    }

    public<F extends Function<S, C>, S extends Serializable, C extends Comparable<C>> ModelDecoratorComparator feedFormerKey(Integer level, F keyGetter) {
        formerKeyMap.put(level, (ModelElement x)-> keyGetter.apply((S)x.getElement()));
        return this;
    }

    public<F extends Function<S, C>, S extends Serializable, C extends Comparable<C>> ModelDecoratorComparator feedLatterKey(Integer level, F keyGetter) {
        latterKeyMap.put(level,  (ModelElement x)-> keyGetter.apply((S)x.getElement()));
        return this;
    }

    public ModelDecoratorComparator feedKeyComparator(Integer level, Comparator keyComparator) {
        keyComparatorMap.put(level,  (ModelElement x, ModelElement y)->
                keyComparator.compare(
                        formerKeyMap.get(level).apply(x),
                        latterKeyMap.get(level).apply(y)));
        return this;
    }

    public ModelDecoratorComparator feedPropComparator(Integer level, BiPredicate propIsEqual) {
        propComparatorMap.put(level,  (ModelElement x, ModelElement y)-> propIsEqual.test(x.getElement(), y.getElement()));
        return this;
    }

    public ModelDiffWrapper compare(ModelDecorator former, ModelDecorator latter) {
        if (former == null || latter == null) { return null; }
        initial(former, latter);
        this.functionLevel0Based = checkLevel0Based(former.getFunctionMap().keySet());
        Diff<ModelElement<?>> elementDiff = compareElementList(former.getElements(), latter.getElements(), 0);
        List<ModelDiffElement<?>> diffElementList = turnDiff2ModelDiffElement(elementDiff, 0);
        List<ModelDiffEdge> diffEdgesList = new LinkedList<>();
        //对比边
        if (former.getEdges().size() == 0 && latter.getEdges().size() == 0){
            // do nothing
        } else {
            Set<Integer> levelSet = new HashSet<>(former.getEdges().size()+ latter.getEdges().size());
            levelSet.addAll(former.getEdges().keySet());
            levelSet.addAll(latter.getEdges().keySet());
            List<Integer> levelList = levelSet.stream().sorted().collect(Collectors.toList());
            for (Integer level: levelList){
                Diff<ModelEdge<?>> edgeDiff =  compareEdge(former.getEdges().get(level), latter.getEdges().get(level));
                ModelDiffEdge diffEdge = new ModelDiffEdge(level, edgeDiff.getCreated(), edgeDiff.getDeleted());
                diffEdgesList.add(diffEdge);
            }
        }
        ModelDiffWrapper wrapper = new ModelDiffWrapper();
        wrapper.setFunctionMap(latter.getFunctionMap());
        wrapper.setElementDiff(diffElementList);
        wrapper.setEdgeDiff(diffEdgesList);
        ModelDiffVisitHelper.removeRemainElement(wrapper);
        return wrapper;
    }

    @SuppressWarnings("Duplicates")
    private void initial(ModelDecorator former, ModelDecorator latter) {
        if (formerKeyMap.isEmpty()) {
            for (Map.Entry<Integer, LevelFunction<Object>> entry : former.getFunctionMap().entrySet()) {
                formerKeyMap.put(entry.getKey(), (ModelElement x)-> entry.getValue().getKeyGetter().apply(x.getElement()));
            }
        }
        if (latterKeyMap.isEmpty()) {
            for (Map.Entry<Integer, LevelFunction<Object>> entry : latter.getFunctionMap().entrySet()) {
                latterKeyMap.put(entry.getKey(), (ModelElement x)-> entry.getValue().getKeyGetter().apply(x.getElement()));
            }
        }
        if (keyComparatorMap.isEmpty()) {
            for (Map.Entry<Integer, LevelFunction<Object>> entry : latter.getFunctionMap().entrySet()) {
                keyComparatorMap.put(entry.getKey(), (ModelElement x, ModelElement y)->formerKeyMap.get(entry.getKey()).apply(x).compareTo(latterKeyMap.get(entry.getKey()).apply(y)));
            }
        }
        if (propComparatorMap.isEmpty()) {
            for (Map.Entry<Integer, LevelFunction<Object>> entry : latter.getFunctionMap().entrySet()) {
                BiPredicate<ModelElement<Object>, ModelElement<Object>> predicate = (ModelElement<Object> x, ModelElement<Object> y)-> Diff.isEqual(x.getElement(), y.getElement(), this.ignoreNull, this.ignoreEmpty, this.caseInSensitive,  entry.getValue().getPropertyGetter());
                propComparatorMap.put(entry.getKey(), predicate);
            }
        }
    }

    private Diff<ModelElement<?>> compareElementList(List<ModelElement<?>> s, List<ModelElement<?>> t, final int level) {
        Comparator formerKeyComparator = Comparator.comparing(formerKeyMap.get(functionLevel0Based.get(level)));
        Comparator latterKeyComparator = Comparator.comparing(latterKeyMap.get(functionLevel0Based.get(level)));
        Comparator keyComparator = keyComparatorMap.get(functionLevel0Based.get(level));
        BiPredicate propPredicate = propComparatorMap.get(functionLevel0Based.get(level));
        return new Diff<ModelElement<?>>()
                .ignoreBlank()
                .keepRemain()
                .diff(s, t,
                        formerKeyComparator,
                        latterKeyComparator,
                        keyComparator,
                        // 比对自身的属性及子节点的属性，version代码的是子节点的属性hash
                        // 这里变更的逻辑比较乱了，原本设想的是version代表的是子节点的变更，然后可以将子节点的版本保存下来，进行快速比对。但是这里因为采用递归的方式，子节点的变更仍然要再获取属性再比对一次
                        propPredicate);
    }

    private Diff<ModelEdge<?>> compareEdge(List<ModelEdge<?>> s, List<ModelEdge<?>> t){
        return new Diff<ModelEdge<?>>().diff(s, t, (ModelEdge<?> e)->String.join(split, e.getSource().toString(), e.getTarget().toString(), e.getExtra()==null?"":e.getExtra().toString()));
    }

    private  List<ModelDiffElement<?>> turnDiff2ModelDiffElement(Diff<ModelElement<?>> diff, int level){
        List<ModelDiffElement<?>> result = new ArrayList<>(diff.getCreated().size() + diff.getUpdated().size() + diff.getDeleted().size());
        for (ModelElement<?> ele: diff.getCreated()){
            ModelDiffElement<?> diffElement = turnCreatedAndDeletedModelElement2ModelDiffElementRecursive(ele, ModelDiffWrapper.State.CREATED);
            result.add(diffElement);
        }
        for (ModelElement<?> ele: diff.getDeleted()){
            ModelDiffElement<?> diffElement = turnCreatedAndDeletedModelElement2ModelDiffElementRecursive(ele, ModelDiffWrapper.State.DELETED);
            result.add(diffElement);
        }
        for (ModelElement<?>[] eles : diff.getUpdated()){
            ModelDiffElement<?> ele = turnUpdatedModelElement2ModelDiffElementRecursive(eles[0], eles[1], level, ModelDiffWrapper.State.UPDATED);
            result.add(ele);
        }
        for (ModelElement<?>[] eles : diff.getRemain()) {
            ModelDiffElement<?> ele = turnUpdatedModelElement2ModelDiffElementRecursive(eles[0], eles[1], level, ModelDiffWrapper.State.REMAIN);
            result.add(ele);
        }
        return result;
    }

    /**
     * 新增或删除的节点，其所有子节点也同为新增或删除状态
     * @param element
     * @param state
     * @return
     */
    private ModelDiffElement<?> turnCreatedAndDeletedModelElement2ModelDiffElementRecursive(ModelElement<?> element, ModelDiffWrapper.State state){
        ModelDiffElement diff = new ModelDiffElement(state, element.getElement());
        if (element.getChildren() != null && element.getChildren().size()>0){
            for (ModelElement<?> child: element.getChildren()){
                diff.addChild(turnCreatedAndDeletedModelElement2ModelDiffElementRecursive(child, state));
            }
        }
        return diff;
    }

    /**
     * 修改的的节点， 需要进一步判断子节点中的新增修改删除状态。
     * @param original
     * @param current
     * @param level
     * @param state
     * @return
     */
    private ModelDiffElement<?> turnUpdatedModelElement2ModelDiffElementRecursive(ModelElement<?> original, ModelElement<?> current, int level, ModelDiffWrapper.State state){
        ModelDiffElement<?> diff = new ModelDiffElement(state,  original.getElement(), current.getElement());
        if ((original.getChildren() == null || original.getChildren().size() == 0)
                && (current.getChildren() == null || current.getChildren().size() == 0) ){
            return diff;
        } else if ((original.getChildren() == null || original.getChildren().size() == 0)){
            for (ModelElement<?> child: current.getChildren()){
                diff.addChild(turnCreatedAndDeletedModelElement2ModelDiffElementRecursive(child, ModelDiffWrapper.State.CREATED));
            }
        } else if (current.getChildren() == null || current.getChildren().size() == 0){
            for (ModelElement<?> child: original.getChildren()){
                diff.addChild(turnCreatedAndDeletedModelElement2ModelDiffElementRecursive(child, ModelDiffWrapper.State.DELETED));
            }
        } else {
            Diff<ModelElement<?>> elementDiff = compareElementList(original.getChildren(), current.getChildren(), level+1);
            List<ModelDiffElement<?>> childrenDiff = turnDiff2ModelDiffElement(elementDiff, level+1);
            diff.addChildren(childrenDiff);
        }
        return diff;
    }

    private static List<Integer> checkLevel0Based(Collection<Integer> levels){
        return levels.stream().sorted().collect(Collectors.toList());
    }

}
