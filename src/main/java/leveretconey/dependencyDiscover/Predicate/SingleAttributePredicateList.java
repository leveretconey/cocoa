package leveretconey.dependencyDiscover.Predicate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import leveretconey.dependencyDiscover.Data.DataFrame;


public class SingleAttributePredicateList implements AbstractPredicateList<SingleAttributePredicate>
,Reversable<SingleAttributePredicateList>
{
    public List<SingleAttributePredicate> list=new ArrayList<>();

    public class TupleComparator implements Comparator<Integer>{

        private DataFrame data;
        public TupleComparator(DataFrame data) {
            this.data = data;
        }

        @Override
        public int compare(Integer tuple1, Integer tuple2) {
            for (SingleAttributePredicate predicate : list) {
                int cell1=data.get(tuple1,predicate.attribute);
                int cell2=data.get(tuple2,predicate.attribute);
                if(cell1!=cell2){
                    return (predicate.operator==Operator.lessEqual || predicate.operator==Operator.less)?
                            cell1-cell2:cell2-cell1;
                }
            }
            return 0;
        }
    }

    public SingleAttributePredicateList(SingleAttributePredicate predicate) {
        list.add(predicate);
    }
    public SingleAttributePredicateList(SingleAttributePredicateList o) {
        list.addAll(o.list);
    }
    public SingleAttributePredicateList() {

    }
    public SingleAttributePredicateList deepClone(){
        return new SingleAttributePredicateList(this);
    }
    @Override
    public Iterator<SingleAttributePredicate> iterator() {
        return list.iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        for (int i=0;i<list.size();i++) {
            if(i!=0)
                sb.append(',');
            sb.append(list.get(i));
        }
        return sb.toString();
    }

    public static SingleAttributePredicateList fromString(String s){
        String[] parts = s.split(",");
        SingleAttributePredicateList list=new SingleAttributePredicateList();
        for (String part : parts) {
            list.list.add(SingleAttributePredicate.fromString(part));
        }
        return list;
    }

    @Override
    public boolean add(SingleAttributePredicate predicate) {
        list.add(predicate);
        return true;
    }

    public SingleAttributePredicate get(int index){
        return list.get(index);
    }

    @Override
    public void remove(SingleAttributePredicate predicate) {
        list.removeIf((p)->p.attribute==predicate.attribute);
    }

    @Override
    public boolean contains(SingleAttributePredicate predicate,boolean isSerious){
        for (SingleAttributePredicate item : list) {
            if (item.attribute==predicate.attribute && ( isSerious || item.operator==predicate.operator))
                return true;
        }
        return false;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof SingleAttributePredicateList))
            return false;
        SingleAttributePredicateList that = (SingleAttributePredicateList) o;
        if (this.size()!=that.size())
            return false;
        int length=this.size();
        for (int i = 0; i < length; i++) {
            if (this.list.get(i) != that.list.get(i)){
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash=1;
        for (SingleAttributePredicate predicate : list) {
            hash= 31 * hash + predicate.hashCode();
        }
        return hash;
    }

    @Override
    public boolean isReverse(SingleAttributePredicateList l){
        if(l==null || list.size()!=l.size())
            return false;

        for (int i = 0; i < list.size(); i++) {
            SingleAttributePredicate p1 = list.get(i);
            SingleAttributePredicate p2 = l.list.get(i);
            if(p1.attribute!=p2.attribute || p1.operator.reverse() != p2.operator)
                return false;
        }
        return true;
    }

    public boolean canOrderTuple(){
        for (SingleAttributePredicate predicate : list) {
            if (!predicate.operator.isLessOrGreater()){
                return false;
            }
        }
        return true;
    }

    public boolean violate(DataFrame data,int tuple1,int tuple2){
        for (SingleAttributePredicate predicate : list) {
            int diff=data.get(tuple2,predicate.attribute)-data.get(tuple1,predicate.attribute);
            if (diff!=0){
                return diff>0 ^ predicate.operator==Operator.lessEqual;
            }
        }
        return false;
    }

    public int compare(DataFrame data,int tuple1,int tuple2){
        for (SingleAttributePredicate predicate : list) {
            int diff=data.get(tuple1,predicate.attribute)-data.get(tuple2,predicate.attribute);
            if (diff!=0){
                if (predicate.operator == Operator.lessEqual)
                    return diff;
                else
                    return -diff;
            }
        }
        return 0;
    }

    public void removeLastElement(){
        if (size()>0){
            list.remove(list.size()-1);
        }
    }
    public boolean isPrefixOf(SingleAttributePredicateList other){
        if (this==other){
            return true;
        }
        if (list.size()>other.size()){
            return false;
        }
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i)!=other.list.get(i)){
                return false;
            }
        }
        return true;
    }

    public SingleAttributePredicateList deepCloneAndRemoveLast(){
        SingleAttributePredicateList result = deepClone();
        result.removeLastElement();
        return result;
    }
    public SingleAttributePredicateList deepCloneAndAdd(SingleAttributePredicate predicate){
        SingleAttributePredicateList list = this.deepClone();
        list.add(predicate);
        return list;
    }

    public SingleAttributePredicateList getReverseList(){
        SingleAttributePredicateList result=new SingleAttributePredicateList();
        for (SingleAttributePredicate predicate : list) {
            result.add(SingleAttributePredicate.getInstance(predicate.attribute,
                    predicate.operator==Operator.greaterEqual?Operator.lessEqual:Operator.greaterEqual));
        }
        return result;
    }
}
