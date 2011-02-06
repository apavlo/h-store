package edu.brown.markov;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.collections15.EnumerationUtils;
import org.apache.commons.collections15.set.ListOrderedSet;

import weka.core.Attribute;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author pavlo
 */
public class MarkovAttributeSet extends ListOrderedSet<Attribute> implements Comparable<MarkovAttributeSet> {
    private static final long serialVersionUID = 1L;
    private Double cost;

    public MarkovAttributeSet(Set<Attribute> items) {
        super(items);
    }
    
    public MarkovAttributeSet(Attribute...items) {
        super((Set<Attribute>)CollectionUtil.addAll(new HashSet<Attribute>(), items));
    }
    
    /**
     * Copy constructor
     * @param clone
     */
    public MarkovAttributeSet(MarkovAttributeSet clone) {
        super(clone);
    }
    
    protected MarkovAttributeSet(Instances data, Collection<Integer> idxs) {
        for (Integer i : idxs) {
            this.add(data.attribute(i));
        } // FOR
    }
    
    @SuppressWarnings("unchecked")
    protected MarkovAttributeSet(Instances data, String prefix) {
        for (Attribute a : (List<Attribute>)EnumerationUtils.toList(data.enumerateAttributes())) {
            if (a.name().startsWith(prefix)) this.add(a);
        } // FOR
    }
    
    public Filter createFilter(Instances data) throws Exception {
        Set<Integer> indexes = new HashSet<Integer>();
        for (int i = 0, cnt = this.size(); i < cnt; i++) {
            indexes.add(this.get(i).index());
        } // FOR
        
        SortedSet<Integer> to_remove = new TreeSet<Integer>(); 
        for (int i = 0, cnt = data.numAttributes(); i < cnt; i++) {
            if (indexes.contains(i) == false) {
                to_remove.add(i+1);
            }
        } // FOR
        
        Remove filter = new Remove();
        filter.setInputFormat(data);
        String options[] = { "-R", StringUtil.join(",", to_remove) };
        filter.setOptions(options);
        return (filter);
    }
//    
//    public Instances copyData(Instances data) throws Exception {
//        Set<Integer> indexes = new HashSet<Integer>();
//        for (int i = 0, cnt = this.size(); i < cnt; i++) {
//            indexes.add(this.get(i).index());
//        } // FOR
//        
//        SortedSet<Integer> to_remove = new TreeSet<Integer>(); 
//        for (int i = 0, cnt = data.numAttributes(); i < cnt; i++) {
//            if (indexes.contains(i) == false) {
//                to_remove.add(i+1);
//            }
//        } // FOR
//        
//        Remove filter = new Remove();
//        filter.setInputFormat(data);
//        filter.setAttributeIndices(StringUtil.join(",", to_remove));
//        for (int i = 0, cnt = data.numInstances(); i < cnt; i++) {
//            filter.input(data.instance(i));
//        } // FOR
//        filter.batchFinished();
//        
//        Instances newData = filter.getOutputFormat();
//        Instance processed;
//        while ((processed = filter.output()) != null) {
//            newData.add(processed);
//        } // WHILE
//        return (newData);
//    }
    
    public Double getCost() {
        return (this.cost);
    }
    public void setCost(Double cost) {
        this.cost = cost;
    }
    @Override
    public int compareTo(MarkovAttributeSet o) {
        if (this.cost != o.cost) {
            return (this.cost != null ? this.cost.compareTo(o.cost) : o.cost.compareTo(this.cost));
        } else if (this.size() != o.size()) {
            return (this.size() - o.size());
        } else if (this.containsAll(o)) {
            return (0);
        }
        for (int i = 0, cnt = this.size(); i < cnt; i++) {
            int idx0 = this.get(i).index();
            int idx1 = o.get(i).index();
            if (idx0 != idx1) return (idx0 - idx1);
        } // FOR
        return (0);
    }
    @Override
    public String toString() {
        return (MarkovAttributeSet.toString(this));
    }
    
    public static String toString(Set<Attribute> attrs) {
        StringBuilder sb = new StringBuilder();
        String add = "[";
        for (Attribute a : attrs) {
            sb.append(add).append(a.name());
            add = ", ";
        }
        sb.append("]");
        return sb.toString();
    }
}
