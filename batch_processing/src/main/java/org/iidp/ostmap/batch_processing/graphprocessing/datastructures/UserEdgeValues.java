package org.iidp.ostmap.batch_processing.graphprocessing.datastructures;

/**
 *
 */
public class UserEdgeValues {
    public int count;

    public UserEdgeValues(int count) {
        this.count = count;
    }

    public String toString() {
        return Integer.toString(count);
    }
}
