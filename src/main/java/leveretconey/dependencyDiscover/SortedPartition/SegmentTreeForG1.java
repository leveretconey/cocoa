package leveretconey.dependencyDiscover.SortedPartition;

import java.util.Collection;

public class SegmentTreeForG1 {

    private SegmentTreeNode root;

    public SegmentTreeForG1(int low, int high){
        root=build(low, high);
    }
    public SegmentTreeForG1(int max){
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
    public int query(int low,int high){
        return root.query(low, high);
    }

    public void insert(int x){
        SegmentTreeNode node=root;
        while (node!=null){
            node.val++;
            node=  ( x <= (node.rangeLow+node.rangeHigh) / 2 )
                    ? node.left :node.right;
        }
    }


    private static class SegmentTreeNode{
        private SegmentTreeNode left;
        private SegmentTreeNode right;
        private int rangeLow;
        private int rangeHigh;
        private int val;

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

        private int query(int queryLow,int queryHigh){
            if(queryLow>rangeHigh || queryHigh<rangeLow)
                return 0;
            else if(queryLow<=rangeLow && queryHigh >= rangeHigh)
                return val;
            else
                return left.query(queryLow,queryHigh) + right.query(queryLow,queryHigh);
        }
    }
}
