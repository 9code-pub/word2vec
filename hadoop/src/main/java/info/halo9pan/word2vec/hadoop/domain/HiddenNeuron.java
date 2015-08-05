package info.halo9pan.word2vec.hadoop.domain;

public class HiddenNeuron extends Neuron{
    
    public double[] hiddenOutputWeights ; //syn1
    
    public HiddenNeuron(int layerSize){
        this.hiddenOutputWeights = new double[layerSize] ;
    }
    
}
