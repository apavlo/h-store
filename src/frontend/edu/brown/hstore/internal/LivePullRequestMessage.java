package edu.brown.hstore.internal;

import edu.brown.hstore.Hstoreservice.LivePullRequest;
import edu.brown.hstore.txns.AbstractTransaction;

public class LivePullRequestMessage extends InternalTxnMessage {

  LivePullRequest livePullRequest;
  
  public LivePullRequestMessage(AbstractTransaction ts, LivePullRequest livePullRequest) {
    super(ts);
    this.livePullRequest = livePullRequest;
  }
  
  public LivePullRequest getLivePullRequest() {
    return this.livePullRequest;
  }

}
