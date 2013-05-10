package edu.brown.hstore.internal;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.LivePullRequest;
import edu.brown.hstore.Hstoreservice.LivePullResponse;
import edu.brown.hstore.txns.AbstractTransaction;

public class LivePullRequestMessage extends InternalTxnMessage {

  LivePullRequest livePullRequest;
  RpcCallback<LivePullResponse> livePullResponseCallback;
  
  public LivePullRequestMessage(AbstractTransaction ts, LivePullRequest livePullRequest,
      RpcCallback<LivePullResponse> livePullResponseCallback) {
    //TODO : Check whether null can be passed
    super(ts);
    this.livePullRequest = livePullRequest;
    this.livePullResponseCallback = livePullResponseCallback;
  }
  
  public LivePullRequest getLivePullRequest() {
    return this.livePullRequest;
  }
  
  public RpcCallback<LivePullResponse> getLivePullResponseCallback(){
    return (this.livePullResponseCallback);
  }

}
