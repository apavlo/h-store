/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.benchmark.bingo;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.benchmark.Verification;
import org.voltdb.benchmark.Verification.Expression;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.ExpressionType;

import edu.brown.api.BenchmarkComponent;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class BingoClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(BingoClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    
    public static enum Transaction {
        CREATE("Create Tournament"),
        DELETE("Delete Tournament"),
        PLAY("Play Round"),
        GETAVGPOT("Get Average Pot Value"),
        ERROR("Client error executing transaction");

        private Transaction(String displayName) { this.displayName = displayName; }
        public final String displayName;
    }

    private final Random random = new Random();

    // an array of tournaments with a randomized starting round to get better
    // tournament progress distribution at startup. The result of this is that
    // the tournaments at startup run for fewer rounds (and so do smaller deletes).
    // All subsequent tournaments, though, run the full number of rounds. Without
    // something like this, tournament creations and deletions are clumpy.
    protected class Tourney {
        int tid;
        boolean initialized = false;
        int round = 0;

        private class Callback implements ProcedureCallback {
            private final Transaction t;

            public Callback(Transaction t) {
                this.t = t;
            }

            @Override
            public void clientCallback(ClientResponse clientResponse) {
                final Status status = clientResponse.getStatus();
                if (status != Status.OK) {
                    if (status == Status.ABORT_CONNECTION_LOST){
                        /*
                         * Status of the last transaction involving the tournament
                         * is unknown it could have committed. Recovery code would
                         * go here.
                         */
                        return;
                    }

                    if (clientResponse.getException() != null) {
                        clientResponse.getException().printStackTrace();
                    }
                    if (debug.val && clientResponse.getStatusString() != null) {
                        LOG.warn(clientResponse.getStatusString());
                    }
                    return;
                }
                synchronized (tournaments) {
                    tournaments.offer(Tourney.this);
                    tournaments.notifyAll();
                }
                incrementTransactionCounter(clientResponse, t.ordinal());
            }

        }
    }

    private final ArrayDeque<Tourney> tournaments = new ArrayDeque<Tourney>();

    public static void main(String[] args) {
        BenchmarkComponent.main(BingoClient.class, args, false);
    }

    public BingoClient(String args[]) {
        super(args);
        Random r = new Random();
        
        // Load our good old friend Mister HStoreConf
        HStoreConf hstore_conf = this.getHStoreConf();
        
        final int num_tournaments = (int)Math.round(BingoConstants.maxTournaments * hstore_conf.client.scalefactor);
        final int num_clients = this.getNumClients();
        final int client_id = this.getClientId();
        for (int i=1; i <= num_tournaments; i++) {
            if (i % num_clients != client_id) continue;
            final Tourney t = new Tourney();
            t.tid = 10000 + i; //  | (getClientId() << 24));
            t.initialized = false;
            t.round = Math.abs(r.nextInt() % BingoConstants.maxRounds);
            tournaments.offer(t);
        }

        // Set up checking
        buildConstraints();
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        int ii = 0;
        for (Transaction transaction : Transaction.values()) {
            names[ii++] = transaction.displayName;
        }
        return names;
    }

    /**
     * Delete all games that may have existed previously
     * @return
     */
    protected boolean cleanupLastRun() {
        try {
            for (Tourney t : tournaments) {
                this.getClientHandle().callProcedure(
                        new NullCallback(),
                        "DeleteTournament",
                        (long)t.tid);
            }
            this.getClientHandle().drain();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    protected void buildConstraints() {
        Expression constraint = null;

        // PlayRound table 0: 0 <= C4 < 100 (max round is 100)
        constraint = Verification.inRange("C4", 0L, 99L);
        addConstraint("PlayRound", 0, constraint);

        // PlayRound table 1: 0 <= R_POT < 900 (because the max round is 100, we
        // only add at most 9 each time)
        Expression r_pot = Verification.inRange("R_POT", 0, 899);
        Expression t_id = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                           "T_ID", 0);
        Expression b_id = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                           "B_ID", 0);
        Expression r_id = Verification.compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                                           "R_ID", 0);
        constraint = Verification.conjunction(ExpressionType.CONJUNCTION_AND,
                                              r_pot, t_id, b_id, r_id);
        addConstraint("PlayRound", 1, constraint);

        // For the tables
        addConstraint("T", 0, t_id);

        constraint = Verification.conjunction(ExpressionType.CONJUNCTION_AND,
                                              t_id, b_id);
        addConstraint("B", 0, constraint);

        constraint = Verification.conjunction(ExpressionType.CONJUNCTION_AND,
                                              r_pot, t_id, r_id);
        addConstraint("R", 0, constraint);
    }

    @Override
    public void runLoop() {
        if (!cleanupLastRun()) {
            return;
        }

        try {
            while (true) {
                invokeBingo();
                this.getClientHandle().backpressureBarrier();
            }
        } catch (IOException e) {
            /*
             * Client has no clean mechanism for terminating with the DB.
             */
            return;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        return invokeBingo();
    }

    private boolean invokeBingo() throws IOException {
        // which Transaction enum value to execute.
        Transaction proc = Transaction.ERROR;
        boolean queued = false;
        Tourney t;
        synchronized (tournaments) {
            while (true) {

                t = tournaments.poll();
                if (t == null) {
                    return false;
                }

                double prob = random.nextDouble();
                if (prob > .7) {
                    break;
                } else {
                    tournaments.offer(t);
                }
            }
        }

        // tournament id does not exist, CREATE
        if (t.initialized == false) {
            final Tourney tourney = t;
            proc = Transaction.CREATE;

            Tourney.Callback callback = t.new Callback(proc) {
                @Override
                public void clientCallback(ClientResponse clientResponse) {
                    if (clientResponse.getStatus() == Status.OK) {
                        tourney.initialized = true;
                    }
                    super.clientCallback(clientResponse);
                }
            };

            if (debug.val)
                LOG.debug("[" + getClientId() + "] CREATE TOURNEY: " + t.tid);
            queued = this.getClientHandle().callProcedure(
                    callback,
                    "CreateTournament",
                    (long)t.tid,
                    (long)BingoConstants.boardsPerTournament);
        }
        // tournament is over, DELETE, mark uninitialized and at round 0.
        else if (!(t.round < BingoConstants.maxRounds)) {
            final Tourney tourney = t;
            proc = Transaction.DELETE;

            Tourney.Callback callback = t.new Callback(proc) {
                @Override
                public void clientCallback(ClientResponse clientResponse) {
                    if (clientResponse.getStatus() == Status.OK) {
                        tourney.initialized = false;
                        tourney.round = 0;
                    }
                    super.clientCallback(clientResponse);
                }
            };

            queued = this.getClientHandle().callProcedure(
                    callback,
                    "DeleteTournament",
                    (long)t.tid);
        }
        // increment round and PLAY
        else {
            final Tourney tourney = t;

            //Occasionally instead of playing a round check the avg pot value
            if (random.nextInt(BingoConstants.GETAVGPOT_MAX) > BingoConstants.GETAVGPOT_MIN) {
                proc = Transaction.GETAVGPOT;

                /*
                 * Retrieve 9 more tournaments to be included in the average
                 */
                final long tourneyIds[] = new long[10];
                final Tourney tourneys[] = new Tourney[9];
                synchronized (tournaments) {
                    for (int ii = 0; ii < 9; ii++) {
                        while (true) {
                            tourneys[ii] = tournaments.poll();
                            if (tourneys[ii] != null) {
                                break;
                            }
                            try {
                                tournaments.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }

                /*
                 * Create an array of the ids to be sent with the invocation
                 * and store the Tourneys themselves in the callback
                 * so they can be placed back into the queue upon completion
                 */
                tourneyIds[0] = t.tid;
                for (int ii = 1; ii < 10; ii++) {
                    tourneyIds[ii] = tourneys[ii - 1].tid;
                }
                Tourney.Callback callback = t.new Callback(proc) {
                    @Override
                    public void clientCallback(ClientResponse clientResponse) {
                        synchronized (tournaments) {
                            for (int ii = 0; ii < tourneys.length; ii++) {
                                tournaments.offer(tourneys[ii]);
                            }
                        super.clientCallback(clientResponse);
                        }
                    }
                };

                queued = this.getClientHandle().callProcedure(
                        callback,
                        "GetAvgPot", tourneyIds);
            } else {
                proc = Transaction.PLAY;

                Tourney.Callback callback = t.new Callback(proc) {
                    @Override
                    public void clientCallback(ClientResponse clientResponse) {
                        if (clientResponse.getStatus() == Status.OK) {
                            tourney.round++;
                        }
                        if (checkTransaction("PlayRound", clientResponse, false, false)) {
                            super.clientCallback(clientResponse);
                        }
                    }
                };

                queued = this.getClientHandle().callProcedure(
                        callback,
                        "PlayRound", (long)t.tid,
                        (long)t.round,
                        "A7 Again!?");
            }
        }

        /*
         * It wasn't successfully queued so put it back in the list of eligible tournaments
         */
        if (!queued) {
            tournaments.offer(t);
        }

        return queued;
    }
}
