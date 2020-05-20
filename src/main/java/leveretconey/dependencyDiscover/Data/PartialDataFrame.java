package leveretconey.dependencyDiscover.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PartialDataFrame extends DataFrame {
    private Set<Integer> originalIndexesAvailable;
    private List<Integer> indexInOriginalDataFrame;
    private DataFrame originalDataFrame;

    public PartialDataFrame(DataFrame dataFrame) {
        this(dataFrame,new HashSet<>());
    }

    public PartialDataFrame(DataFrame dataFrame, Set<Integer> originalIndexes) {
        super();
        this.setColumnNames(dataFrame.getColumnNames());
        this.originalDataFrame=dataFrame;
        this.originalIndexesAvailable =originalIndexes;
        this.indexInOriginalDataFrame =new ArrayList<>(originalIndexes);
        for (int row : originalIndexes) {
            data.add(originalDataFrame.getTuple(row));
        }
    }

    public boolean containRowInOriginalDaraFrame(int row){
        return originalIndexesAvailable.contains(row);
    }

    public void addRow(int row){
        if(!originalIndexesAvailable.contains(row)){
            originalIndexesAvailable.add(row);
            data.add(originalDataFrame.getTuple(row));
            indexInOriginalDataFrame.add(row);
        }
    }

    public void addRowsFromOriginalDataFrame(Collection<Integer> rows){
        for (Integer row : rows) {
            addRow(row);
        }
    }


    public int getIndexInOriginalDataFrame(int row){
        return indexInOriginalDataFrame.get(row);
    }

    @Override
    public void deleteRow(int row) {
        throw new RuntimeException("method not supported");
    }

    public DataFrame toDataFrame(){
        DataFrame dataFrame=new DataFrame();
        dataFrame.setColumnNames(getColumnNames());
        dataFrame.data=data;
        return dataFrame;
    }
}
