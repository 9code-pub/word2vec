package info.halo9pan.word2vec.hadoop.domain;

public abstract class Neuron implements Comparable<Neuron> {
    public long frequency; //词频
    public Neuron parent;
    public short code; //编码
    
    public int compareTo(Neuron o) {
        if (this.frequency > o.frequency) {
            return 1;
        } else {
            return -1;
        }
    }

}
