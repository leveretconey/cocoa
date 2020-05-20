package leveretconey.cocoa.sample;

import java.util.ArrayList;
import java.util.List;

import javafx.util.Pair;
import leveretconey.cocoa.twoSideExpand.ALODTree;
import leveretconey.cocoa.twoSideExpand.ALODTree.ALODTreeNode;
import leveretconey.dependencyDiscover.Dependency.LexicographicalOrderDependency;
import leveretconey.dependencyDiscover.SPCache.SortedPartitionCache;
import leveretconey.dependencyDiscover.SortedPartition.ImprovedTwoSideSortedPartition;

public class DFSISPCache {
    private List<Pair<LexicographicalOrderDependency,ImprovedTwoSideSortedPartition>> ispCache
            =new ArrayList<>();
    private SortedPartitionCache spCache;

    public DFSISPCache(SortedPartitionCache spCache) {
        this.spCache = spCache;
    }

    public ImprovedTwoSideSortedPartition getISP(LexicographicalOrderDependency dependency){
        ImprovedTwoSideSortedPartition isp=new ImprovedTwoSideSortedPartition(
                spCache.get(dependency.left),spCache.get(dependency.right));
        int insertPosition=0;
        if (!ispCache.isEmpty() && dependency.isPrefixOf(peekDependency())){
            for (int i = ispCache.size()-1; i >= 0; i--) {
                int diff=ispCache.get(i).getKey().length() - dependency.length();
                if (diff<=0){
                    if (diff==0){
                        insertPosition=i;
                    }else {
                        insertPosition=i+1;
                    }
                    break;
                }
            }
        }else {
            while (!ispCache.isEmpty() && !peekDependency().isPrefixOf(dependency)){
                ispCache.remove(ispCache.size()-1);
            }
            insertPosition=ispCache.size();
        }

        if (insertPosition==ispCache.size() || ispCache.get(insertPosition).getKey().length()!=dependency.length()){
            ispCache.add(insertPosition,new Pair<>(dependency,isp));
        }
        for(int i=insertPosition-1;i>=0;i--){
            Pair<LexicographicalOrderDependency, ImprovedTwoSideSortedPartition> pair = ispCache.get(i);
            if (pair.getValue().hasResultCache()){
                if ( pair.getKey().left.size()==dependency.left.size() ){
                    isp.parent=pair.getValue();
                    isp.expandLeft=false;
                }else if (pair.getKey().right.size() == dependency.right.size()){
                    isp.parent=pair.getValue();
                    isp.expandLeft=true;
                }
                break;
            }
        }
        return isp;
    }

    private LexicographicalOrderDependency peekDependency(){
        return ispCache.get(ispCache.size()-1).getKey();
    }
}
