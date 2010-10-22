package edu.mit.hstore;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.mit.dtxn.Dtxn;
import edu.mit.dtxn.Dtxn.CoordinatorFragment;
import edu.mit.dtxn.Dtxn.CoordinatorResponse;
import edu.mit.dtxn.Dtxn.FinishRequest;
import edu.mit.dtxn.Dtxn.FinishResponse;

public class MockDtxnCoordinator extends Dtxn.Coordinator {
    public CoordinatorFragment request;
    public RpcCallback<CoordinatorResponse> done;

    @Override
    public void execute(RpcController controller,
            CoordinatorFragment request,
            RpcCallback<CoordinatorResponse> done) {
        this.request = request;
        this.done = done;
    }

    @Override
    public void finish(RpcController controller, FinishRequest request, RpcCallback<FinishResponse> done) {
        done.run(FinishResponse.newBuilder().build());
    }
}