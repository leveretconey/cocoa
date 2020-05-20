package leveretconey.dependencyDiscover.SortedPartition;

import java.util.Collection;

public class SegmentTreeForG3 {

    public enum Option{
        ERROR_RATE,LOWER_BOUND,UPPER_BOUND,
    }

    private SegmentTreeNode root;

    public SegmentTreeForG3(int low, int high){
        root=build(low, high);
    }
    public SegmentTreeForG3(int max){
        this(0,max);
    }
    private SegmentTreeNode build(int low,int high){
        SegmentTreeNode node=new SegmentTreeNode(low,high);
        if(low!=high) {
            int mid = (low + high) / 2;
            node.left=build(low,mid);
            node.right=build(mid+1,high);
        }
        return node;
    }
    public int query(int low,int high,Option option){
        if (high<low){
            return 0;
        }
        return root.query(low, high,option.ordinal());
    }
    public int query(int low,int high){
        return query(low, high,Option.ERROR_RATE);
    }

    public void insert(int x,int y,Option option){
        SegmentTreeNode node=root;
        int index = option.ordinal();
        while (node!=null){
            if (node.val[index] < y){
                node.val[index] = y;
            }
            node=  ( x <= (node.rangeLow+node.rangeHigh) / 2 ) ? node.left :node.right;
        }
    }
    public void insert(int x,int y){
        insert(x,y,Option.ERROR_RATE);
    }


    private static class SegmentTreeNode{
        private SegmentTreeNode left;
        private SegmentTreeNode right;
        private int rangeLow;
        private int rangeHigh;
        private int[] val=new int[3];

        private SegmentTreeNode(int rangeLow, int rangeHigh) {
            this.rangeLow = rangeLow;
            this.rangeHigh = rangeHigh;
        }

        private SegmentTreeNode(SegmentTreeNode left, SegmentTreeNode right, int rangeLow, int rangeHigh) {
            this.left = left;
            this.right = right;
            this.rangeLow = rangeLow;
            this.rangeHigh = rangeHigh;
        }

        private int query(int queryLow,int queryHigh,int index){
            if(queryLow>rangeHigh || queryHigh<rangeLow)
                return 0;
            else if(queryLow<=rangeLow && queryHigh >= rangeHigh)
                return val[index];
            else
                return Math.max(left.query(queryLow,queryHigh,index) , right.query(queryLow,queryHigh,index));
        }
    }
}
