package leveretconey.dependencyDiscover.AnalysisTools;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import leveretconey.util.Util;

public class FileComparator {
    public void compareTwoFiles(String filePath1,String filePath2){
        Set<String> lines1=getFileLinesSet(filePath1);
        Set<String> lines2=getFileLinesSet(filePath2);

        Util.out(String.format("%s在%s中没有的行：",filePath1,filePath2));
        for (String line : lines1) {
            if (!lines2.contains(line)){
                Util.out(line);
            }
        }

        Util.out(String.format("\n%s在%s中没有的行：",filePath2,filePath1));
        for (String line : lines2) {
            if (!lines1.contains(line)){
                Util.out(line);
            }
        }
    }

    @SuppressWarnings("all")
    private Set<String> getFileLinesSet(String filePath){
        String[] lines= Util.fromFile(filePath).split("\\n");
        Set<String> result=new HashSet<>();
        for (String line : lines) {
            result.add(line);
        }
        return result;
    }

    public static void main(String[] args) {
        FileComparator comparator=new FileComparator();
        comparator.compareTwoFiles("g3.txt","single g3.txt");
    }
}
