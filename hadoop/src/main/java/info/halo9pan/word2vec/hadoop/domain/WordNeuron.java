package info.halo9pan.word2vec.hadoop.domain;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class WordNeuron extends Neuron {
    public String name;
    public double[] inputHiddenWeights = null; //syn0
    public List<Neuron> neurons = null;//路径神经元
    public int[] codeArr = null;

    public List<Neuron> makeNeurons() {
        if (neurons != null) {
            return neurons;
        }
        Neuron neuron = this;
        neurons = new LinkedList<>();
        while ((neuron = neuron.parent) != null) {
            neurons.add(neuron);
        }
        Collections.reverse(neurons);
        codeArr = new int[neurons.size()];

        for (int i = 1; i < neurons.size(); i++) {
            codeArr[i - 1] = neurons.get(i).code;
        }
        codeArr[codeArr.length - 1] = this.code;

        return neurons;
    }

    public WordNeuron(String name, int frequency, int layerSize) {
        this.name = name;
        this.frequency = frequency;
        this.inputHiddenWeights = new double[layerSize];
        Random random = new Random();
        for (int i = 0; i < inputHiddenWeights.length; i++) {
            inputHiddenWeights[i] = (random.nextDouble() - 0.5) / layerSize;
        }
    }

}