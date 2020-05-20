package leveretconey.dependencyDiscover.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javafx.util.Pair;
import leveretconey.util.Util;

public class DataFormatConverter {

    private static final Type[] supportedTypes=new Type[]
            {new TypeNull(),new TypeLong(),new TypeDouble(),new TypeString()};

    private static class DataFrame{
        //和另一个dataframe唯一的区别在于，这个是列优先的,用作数据的预处理
        private String[] heads;
        private Integer[][] data;

        public int getTupleCount(){
            if(data.length>0)
                return data[0].length;
            else
                return 0;
        }
        public int getColumnCount(){
            return heads.length;
        }

        @Override
        public String toString() {
            int tupleCount= getTupleCount();
            int columnCount= getColumnCount();
            StringBuilder sb=new StringBuilder();
            for (int i=0;i<columnCount;i++) {
                if(i!=0)
                    sb.append(',');
                sb.append(heads[i]);
            }
            for (int tuple = 0; tuple < tupleCount; tuple++) {
                sb.append('\n');
                for (int column = 0; column < columnCount; column++) {
                    if(column!=0)
                        sb.append(',');
                    sb.append(data[column][tuple]);
                }
            }
            return sb.toString();
        }
        private void filter(DataFramePostFilter filter){
            DataConverterFilterResult filterResult=filter.filter(this.data);
            if (filterResult==null)
                return;
            boolean deleteColumn=filterResult.columnsToDelete.size()>0;
            boolean deleteTuple=filterResult.tuplesToDelete.size()>0;
            if(!deleteColumn && !deleteTuple){
                return;
            }
            int oldColumnCount= getColumnCount();
            int oldTupleCount= getTupleCount();
            int newColumnCount =oldColumnCount-filterResult.columnsToDelete.size();
            int newTupleCount =oldTupleCount-filterResult.tuplesToDelete.size();

            Util.out(String.format("post filter: delete %d columns and %d tuples"
                    ,filterResult.columnsToDelete.size(),filterResult.tuplesToDelete.size()));
            //head
            if(deleteColumn){
                String[] newHead=new String[newColumnCount];
                int fillPointer=0;
                for (int column = 0; column < oldColumnCount; column++) {
                    if(!filterResult.columnsToDelete.contains(column)){
                        newHead[fillPointer++]=heads[column];
                    }
                }
                heads=newHead;
            }
            //data
            Integer[][] newData;

            if(deleteTuple){
                newData=new Integer[newColumnCount][newTupleCount];
                int columnFillPointer=0;
                for (int column = 0; column < oldColumnCount; column++) {
                    if(filterResult.columnsToDelete.contains(column)) {
                        data[column]=null;
                        continue;
                    }
                    int tupleFillPointer=0;
                    for(int tuple=0;tuple<oldTupleCount;tuple++){
                        if(!filterResult.tuplesToDelete.contains(tuple)){
                            newData[columnFillPointer][tupleFillPointer++]=data[column][tuple];
                        }
                    }
                    columnFillPointer++;
                }
            }else {
                newData=new Integer[newColumnCount][];
                int fillPointer=0;
                for (int column = 0; column < oldColumnCount; column++) {
                    if(filterResult.columnsToDelete.contains(column)){
                        data[column]=null;
                    }else {
                        newData[fillPointer++]=data[column];
                    }
                }
            }
            data=newData;
        }

        public void postFilterRearrangeData() {
            for (int i = 0; i < heads.length; i++) {
                data[i]=reassignValueByOrder(data[i],(i1,i2)->(Integer)i1-(Integer)i2);
            }
        }
    }

    public static class DataConverterFilterResult {
        public Set<Integer> columnsToDelete;
        public Set<Integer> tuplesToDelete;

        public DataConverterFilterResult(Set<Integer> columnsToDelete, Set<Integer> tuplesToDelete) {
            this.columnsToDelete = columnsToDelete;
            this.tuplesToDelete = tuplesToDelete;
        }

        public DataConverterFilterResult() {
            columnsToDelete=new HashSet<>();
            tuplesToDelete=new HashSet<>();
        }
    }
    public interface DataFramePostFilter {
        DataConverterFilterResult filter(Integer[][] data);
    }
    public interface DataFramePreFilter {
        DataConverterFilterResult filter(String[][] data);
    }

    public static class DataFormatConverterConfig{
        public String inputPath;
        public String outputPath;
        public boolean hasHead=true;
        public Character delimeter;
        public DataFramePreFilter[] preFilters;
        public DataFramePostFilter[] postFilters;

        public DataFormatConverterConfig(String inputPath) {
            this.inputPath = inputPath;
        }
        private void autoJudge(String firstLine){
            if(outputPath==null) {
                int lastDotPosition = inputPath.lastIndexOf('.');
                if (lastDotPosition == -1)
                    outputPath = inputPath + " converted";
                else
                    outputPath = inputPath.substring(0, lastDotPosition) + " converted." + inputPath.substring(lastDotPosition + 1);
            }
            if(delimeter==null)
                delimeter=testDelimeter(firstLine);
        }

        private char testDelimeter(String line){
            int countComma=0;
            int countSemicolon=0;
            for (char c : line.toCharArray()) {
                if (c==',')
                    countComma++;
                else if(c==';')
                    countSemicolon++;
            }
            return countComma>countSemicolon ? ',' : ';' ;
        }
    }

    public void convert(DataFormatConverterConfig config){
        try {
            //infer basic information
            DataFrame convertedDataFrame=new DataFrame();
            String fileString= Util.fromFile(config.inputPath);
            String[] lines=fileString.split("\n");
            String firstLine=lines[0];
            config.autoJudge(firstLine);
            int tupleCount=lines.length;
            if(config.hasHead)
                tupleCount--;
            int columnCount=splitByCharConsideringQuotationMark
                    (lines[0],config.delimeter,Integer.MIN_VALUE).length;

            //make Head
            if(config.hasHead){
                convertedDataFrame.heads=splitByCharConsideringQuotationMark
                        (lines[0],config.delimeter,columnCount);
                for (int i = 0; i < columnCount; i++) {
                    if ("".equals(convertedDataFrame.heads[i])){
                        convertedDataFrame.heads[i]=String.format("%s(%d)",Util.excelStyleColumnName(i+1),i+1);
                    }
                }
            }else {
                convertedDataFrame.heads=new String[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    convertedDataFrame.heads[i-1]=String.format("%s(%d)"
                            ,Util.excelStyleColumnName(i),i);
                }
            }
            //make cells
            String[][] cells=new String[columnCount][tupleCount];
            for(int i=config.hasHead?1:0;i<lines.length;i++){
                int tuple=config.hasHead?i-1:i;
                String[] lineParts=splitByCharConsideringQuotationMark(lines[i],config.delimeter,columnCount);
                lines[i]=null;
                for (int column = 0; column < columnCount; column++) {
                    cells[column][tuple]=lineParts[column]==null?"":lineParts[column];
                }
            }

            //pre filters
            if (config.preFilters!=null && config.preFilters.length!=0){
                for (DataFramePreFilter preFilter : config.preFilters) {
                    DataConverterFilterResult filterResult=preFilter.filter(cells);
                    if (filterResult==null)
                        continue;
                    boolean deleteColumn=filterResult.columnsToDelete.size()>0;
                    boolean deleteTuple=filterResult.tuplesToDelete.size()>0;
                    if(!deleteColumn && !deleteTuple){
                        continue;
                    }
                    int oldColumnCount= columnCount;
                    int oldTupleCount= tupleCount;
                    columnCount =oldColumnCount-filterResult.columnsToDelete.size();
                    tupleCount =oldTupleCount-filterResult.tuplesToDelete.size();
                    Util.out(String.format("pre filter: delete %d columns and %d tuples"
                            ,filterResult.columnsToDelete.size(),filterResult.tuplesToDelete.size()));
                    //head
                    if(deleteColumn){
                        String[] newHead=new String[columnCount];
                        int fillPointer=0;
                        for (int column = 0; column < oldColumnCount; column++) {
                            if(!filterResult.columnsToDelete.contains(column)){
                                newHead[fillPointer++]=convertedDataFrame.heads[column];
                            }
                        }
                        convertedDataFrame.heads=newHead;
                    }
                    //data
                    String[][] newCells;

                    if(deleteTuple){
                        newCells=new String[columnCount][tupleCount];
                        int columnFillPointer=0;
                        for (int column = 0; column < oldColumnCount; column++) {
                            if(filterResult.columnsToDelete.contains(column)) {
                                cells[column]=null;
                                continue;
                            }
                            int tupleFillPointer=0;
                            for(int tuple=0;tuple<oldTupleCount;tuple++){
                                if(!filterResult.tuplesToDelete.contains(tuple)){
                                    newCells[columnFillPointer][tupleFillPointer++]=cells[column][tuple];
                                }
                            }
                            columnFillPointer++;
                        }
                    }else {
                        newCells=new String[columnCount][];
                        int fillPointer=0;
                        for (int column = 0; column < oldColumnCount; column++) {
                            if(filterResult.columnsToDelete.contains(column)){
                                cells[column]=null;
                            }else {
                                newCells[fillPointer++]=cells[column];
                            }
                        }
                    }
                    cells=newCells;
                }
            }

            // type inference and convert
            convertedDataFrame.data=new Integer[columnCount][];
            for (int column = 0; column < columnCount; column++) {
                convertedDataFrame.data[column]=inferAndConvertColumn(cells[column]);
                cells[column]=null;
            }

            //post filters
            if(config.postFilters !=null && config.postFilters.length!=0){
                for (DataFramePostFilter filter : config.postFilters) {
                    convertedDataFrame.filter(filter);
                }
                convertedDataFrame.postFilterRearrangeData();
            }

            Util.toFile(convertedDataFrame.toString(),config.outputPath);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    private String[] splitByCharConsideringQuotationMark(String s,char delimeter,int minLength){
        List<Integer> delimeterPositions=new ArrayList<>();
        delimeterPositions.add(-1);
        boolean inQuotation=false;
        for (int i=0;i<s.length();i++) {
            char c=s.charAt(i);
            if(c==delimeter && !inQuotation)
                delimeterPositions.add(i);
            else if(c=='"')
                inQuotation=!inQuotation;
        }
        delimeterPositions.add(s.length());
        String[] result=new String[Math.max(minLength,delimeterPositions.size()-1)];
        for (int i = 0; i < delimeterPositions.size()-1; i++) {
            int low=delimeterPositions.get(i)+1,high=delimeterPositions.get(i+1)-1;
            if(low+1<=high && s.charAt(low)=='"' && s.charAt(high)=='"')
                result[i]=s.substring(low+1,high);
            else
                result[i]=s.substring(low,high+1);
        }
        return result;
    }

    private Integer[] inferAndConvertColumn(String[] column){
        if(column.length==0)
            return new Integer[0];
        int typePointer=0;
        int lastTypePointer=supportedTypes.length-1;
        int length=column.length;

        //inferType
        for (int i=0;i<length;i++) {
            String cell=column[i];
            while (typePointer!=lastTypePointer && !supportedTypes[typePointer].fitFormat(cell))
                typePointer++;
            if(typePointer==lastTypePointer)
                break;
        }
        Type inferredType=supportedTypes[typePointer];

        //convert string to its true type
        Object[] convertedData=new Object[length];

        for (int i=0;i<length;i++) {
            convertedData[i] =inferredType.parse(column[i]);
        }

        //sort and give new value
        Comparator comparator=inferredType.getComparator();
        return reassignValueByOrder(convertedData,comparator);
    }

    @SuppressWarnings("unchecked")
    private static Integer[] reassignValueByOrder(Object[] objects, Comparator comparator){
        int length=objects.length;
        Integer[] convertedIntegers=new Integer[length];
        Pair<Integer,Object>[] indexObjectPairs=new Pair[length];
        for (int i = 0; i < length; i++) {
            indexObjectPairs[i]=new Pair<>(i,objects[i]);
        }

        Arrays.sort(indexObjectPairs,(p1,p2)->comparator.compare(p1.getValue(),p2.getValue()));

        convertedIntegers[indexObjectPairs[0].getKey()]=0;
        for (int i=1;i<length;i++) {
            Pair<Integer,Object> pair=indexObjectPairs[i];
            Pair<Integer,Object> lastPair=indexObjectPairs[i-1];
            convertedIntegers[pair.getKey()]= convertedIntegers[lastPair.getKey()] +
                    (pair.getValue().equals(lastPair.getValue()) ? 0:1);
        }
        return convertedIntegers;
    }


    private static final double NULL_THRESHOLD_COLUMN=0.8;
    private static final double NULL_THRESHOLD_TUPLE=0.5;
    public static DataFramePostFilter singleOrdinarityColumnPostFilter =(data)->{
        DataConverterFilterResult result=new DataConverterFilterResult();
        int countColumn=data.length;
        if (countColumn==0)
            return null;
        int countTuple=data[0].length;
        if (countTuple==0)
            return null;
        for (int column = 0; column < countColumn; column++) {
            int min=data[column][0],max=data[column][0];
            for (int tuple = 1; tuple < countTuple; tuple++) {
                min=Math.min(min,data[column][tuple]);
                max=Math.max(max,data[column][tuple]);
            }
            if (min==max)
                result.columnsToDelete.add(column);
        }
        return result;
    };
    public static DataFramePreFilter tooManyNullTuplePreFilter =(data)->{
        DataConverterFilterResult result=new DataConverterFilterResult();
        int countColumn=data.length;
        if (countColumn==0)
            return null;
        int countTuple=data[0].length;
        if (countTuple==0)
            return null;
        int zeroCountThreshold=(int)(NULL_THRESHOLD_TUPLE * countTuple);
        for (int tuple = 0; tuple < countTuple; tuple++) {
            int zeroCount=0;
            for (int column = 0; column < countColumn; column++) {
                if (data[column][tuple]==null || "".equals(data[column][tuple]))
                    zeroCount++;
            }
            if (zeroCount>zeroCountThreshold)
                result.columnsToDelete.add(tuple);
        }
        
        return result;
    };
    public static DataFramePreFilter tooManyNullColumnPreFilter =(data)->{
        DataConverterFilterResult result=new DataConverterFilterResult();
        int countColumn=data.length;
        if (countColumn==0)
            return null;
        int countTuple=data[0].length;
        if (countTuple==0)
            return null;
        int zeroCountThreshold=(int)(NULL_THRESHOLD_COLUMN * countColumn);
        for (int column = 0; column < countColumn; column++) {
            int zeroCount=0;
            for (int tuple = 0; tuple < countTuple; tuple++) {
                if (data[column][tuple]==null || "".equals(data[column][tuple]))
                    zeroCount++;
            }
            if (zeroCount>zeroCountThreshold)
                result.columnsToDelete.add(column);
        }

        return result;
    };


    public static final double CONSTANT_THRESHOLD=0.96;
    public static DataFramePostFilter nearlyConstantColumnFilter =(data)->{
        DataConverterFilterResult result=new DataConverterFilterResult();
        int countColumn=data.length;
        if (countColumn==0)
            return null;
        int countTuple=data[0].length;
        if (countTuple==0)
            return null;
        int maxDuplicateCount=(int)(countTuple * CONSTANT_THRESHOLD);

        nextColumn:
        for (int column = 0; column < countColumn; column++) {
            HashMap<Integer,Integer> value2count=new HashMap<>();
            for (int tuple = 0; tuple < countTuple; tuple++) {
                int value=data[column][tuple];
                value2count.put(value,value2count.getOrDefault(value,0)+1);
            }
            for (int count : value2count.values()) {
                if (count>=maxDuplicateCount){
                    result.columnsToDelete.add(column);
                    continue nextColumn;
                }
            }

        }
        return result;
    };

    public static DataFramePostFilter getPostRandomFilter(int countTuple, int countColumn){
        return data -> {
            DataConverterFilterResult result = new DataConverterFilterResult();
            result.columnsToDelete.addAll(Util.randomSample(data.length,countColumn));
            result.tuplesToDelete.addAll(Util.randomSample(data[0].length,countTuple));
            return result;
        };
    }
}
