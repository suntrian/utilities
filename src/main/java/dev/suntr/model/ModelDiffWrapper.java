package dev.suntr.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author suntr
 * @version dmp1.6.0
 * 整个模型的变更信息包装类
 */
public class ModelDiffWrapper implements Serializable {

    private static final long serialVersionUID = -3169215895599226744L;

    public enum State implements Serializable{
        CREATED, UPDATED, DELETED, REMAIN
    }

    private Map<Integer, LevelFunction<Object>> functionMap;

    private List<ModelDiffElement<?>> elementDiff;

    private List<ModelDiffEdge> edgeDiff;

    public boolean isEmpty() {
        return elementDiff == null || elementDiff.isEmpty();
    }

    public Map<Integer, LevelFunction<Object>> getFunctionMap() {
        return functionMap;
    }

    public List<ModelDiffElement<?>> getElementDiff() {
        return elementDiff;
    }

    public List<ModelDiffEdge> getEdgeDiff() {
        return edgeDiff;
    }

    public void setFunctionMap(Map<Integer, LevelFunction<Object>> functionMap) {
        this.functionMap = functionMap;
    }

    public void setElementDiff(List<ModelDiffElement<?>> elementDiff) {
        this.elementDiff = elementDiff;
    }

    public void setEdgeDiff(List<ModelDiffEdge> edgeDiff) {
        this.edgeDiff = edgeDiff;
    }
}
