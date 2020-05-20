package leveretconey.cocoa.sample;

import java.util.ArrayList;
import java.util.List;

import javafx.util.Pair;
import leveretconey.cocoa.twoSideExpand.ALODTree;
import leveretconey.cocoa.twoSideExpand.ALODTree.ALODTreeNode;
import leveretconey.cocoa.twoSideExpand.TwoSideDFSSPCache;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.SPCache.SortedPartitionCache;
import leveretconey.dependencyDiscover.SortedPartition.ImprovedTwoSideSortedPartition;

public class DFSISPCacheAttachedToNode {
    private List<Pair<ALODTreeNode,ImprovedTwoSideSortedPartition>> ispCache
            =new ArrayList<>();
    private TwoSideDFSSPCache spCache;


    public DFSISPCacheAttachedToNode(TwoSideDFSSPCache spCache) {
        this.spCache = spCache;
    }

    public ImprovedTwoSideSortedPartition getISP(ALODTreeNode node){
        int nodeIndex;
        for( nodeIndex=ispCache.size()-1;;nodeIndex--){
            if (ispCache.get(nodeIndex).getKey()==node){
                break;
            }
        }
        Pair<ALODTreeNode, ImprovedTwoSideSortedPartition> pair1 = ispCache.get(nodeIndex);
        if (pair1.getValue()!=null){
            return pair1.getValue();
        }
        ImprovedTwoSideSortedPartition isp=new ImprovedTwoSideSortedPartition(
                spCache.get(node.toLOD().left), spCache.get(node.toLOD().right));
        ispCache.set(nodeIndex,new Pair<>(node,isp));
        for (int parentIndex=nodeIndex-1;parentIndex>=0;parentIndex--){
            Pair<ALODTreeNode, ImprovedTwoSideSortedPartition> pair2 = ispCache.get(parentIndex);
            if (pair2.getValue()!=null && pair2.getValue().hasResultCache()){
                if (pair1.getKey().toLOD().left.size()==pair2.getKey().toLOD().left.size()){
                   isp.parent= pair2.getValue();
                   isp.expandLeft=false;
                }else if (pair1.getKey().toLOD().right.size()==pair2.getKey().toLOD().right.size()) {
                    isp.parent= pair2.getValue();
                    isp.expandLeft=true;
                }
                break;
            }
        }
        return isp;
    }

    public void updateWorkingNode(ALODTreeNode node){
        if (ispCache.isEmpty()){
            ispCache.add(new Pair<>(node,null));
            return;
        }
        while (!ispCache.isEmpty() && !peekCache().getKey().isAccessible()){
            ispCache.remove(ispCache.size()-1);
        }
        if (node.ancestorOf(peekCache().getKey())){
            return;
        }
        while (!ispCache.isEmpty()){
            ALODTreeNode peekNode = peekCache().getKey();
            if (peekNode==node){
                return;
            }else if(peekNode.ancestorOf(node)){
                break;
            }else {
                spCache.mayRemove(peekNode.expandedSide());
                ispCache.remove(ispCache.size()-1);
            }
        }
        ispCache.add(new Pair<>(node,null));
    }

    private Pair<ALODTreeNode,ImprovedTwoSideSortedPartition> peekCache(){
        return ispCache.get(ispCache.size()-1);
    }

    public void clear(){
        for (Pair<ALODTreeNode, ImprovedTwoSideSortedPartition> pair : ispCache) {
            spCache.mayRemove(pair.getKey().expandedSide());
        }
        ispCache.clear();
    }
}
