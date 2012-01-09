/**
 *
 */
package edu.brown.protorpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.RpcCallback;

public final class RpcCallbackQueue<ParameterType> implements RpcCallback<ParameterType> {
    @Override
    public void run(ParameterType parameter) {
        result.add(parameter);
    }

    public ParameterType getResult() {
        if (result.isEmpty()) return null;
        return result.get(result.size()-1);
    }

    public List<ParameterType> getResults() {
        return Collections.unmodifiableList(result);
    }

    public void reset() {
        result.clear();
    }

    private final ArrayList<ParameterType> result = new ArrayList<ParameterType>();
}
