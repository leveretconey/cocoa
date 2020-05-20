package leveretconey.fastod;

import java.util.Collection;
import java.util.Iterator;

public class AttributeSet implements Iterable<Integer>{
    private final int value;

    public int getValue() {
        return value;
    }

    public AttributeSet() {
        this(0) ;
    }

    public AttributeSet(int value) {
        this.value = value;
    }
    public AttributeSet(Collection<Integer> attributes){
        int sum=0;
        for(int attribute:attributes){
            sum+=1<<attribute;
        }
        value=sum;
    }

    @Override
    public Iterator<Integer> iterator() {
        return new AttributeSetIterator();
    }

    public boolean containAttribute(int attribute){
        return (value & 1<<attribute)!=0;
    }

    public AttributeSet addAttribute(int attribute){
        if(containAttribute(attribute)){
            return this;
        }else {
            return new AttributeSet(value | 1<<attribute);
        }
    }

    public AttributeSet deleteAttribute(int attribute){
        if(containAttribute(attribute)){
            return new AttributeSet(value ^ 1<<attribute);
        }else {
            return this;
        }
    }



    public class AttributeSetIterator implements Iterator<Integer>{
        private int pointer;

        public AttributeSetIterator() {
            pointer=-1;
            findNext();
        }

        private void findNext(){
            pointer++;
            while (pointer<32 && !containAttribute(pointer)){
                pointer++;
            }
        }
        @Override
        public boolean hasNext() {
            return pointer<32;
        }

        @Override
        public Integer next() {
            int result=pointer;
            findNext();
            return result;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append('{');
        boolean first=true;
        for (int attribute : this) {
            if(first)
                first=false;
            else
                sb.append(',');
            sb.append(attribute+1);
        }
        sb.append('}');
        return sb.toString();
    }

    public AttributeSet union(AttributeSet as2){
        return new AttributeSet(value | as2.value);
    }

    public AttributeSet intersect(AttributeSet as2){
        return new AttributeSet(value & as2.value);
    }
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AttributeSet that = (AttributeSet) o;
        return value == that.value;
    }
    public int getAttributeCount(){
        return Integer.bitCount(value);
    }
    @Override
    public int hashCode() {
        return value;
    }


    public int getFirstAttribute(){
        for (int i = 0; i < 32; i++) {
            if(containAttribute(i))
                return i;
        }
        throw new RuntimeException("找不到属性");
    }

    public int getLastAttribute(){
        for (int i = 31; i >= 0; i--) {
            if(containAttribute(i))
                return i;
        }
        throw new RuntimeException("找不到属性");
    }

    public AttributeSet difference(AttributeSet as2){
        return new AttributeSet( value & ( ~0 ^ as2.value));
    }

    public boolean isEmpty(){
        return value==0;
    }

    public boolean subsetOf(AttributeSet as2){
        return (value | as2.value) == as2.value;
    }

    public boolean superSet(AttributeSet as2){
        return (value | as2.value) == value;
    }
}
