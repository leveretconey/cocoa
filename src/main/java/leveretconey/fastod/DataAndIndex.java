package leveretconey.fastod;

class DataAndIndex implements Comparable<DataAndIndex> {
    public int data;
    public int index;

    @Override
    public int compareTo(DataAndIndex o) {
        return data-o.data;
    }

    @Override
    public String toString() {
        return "DataAndIndex{" +
                "data=" + data +
                ", index=" + index +
                '}';
    }

    public DataAndIndex(int data, int index) {
        this.data = data;
        this.index = index;
    }
}
