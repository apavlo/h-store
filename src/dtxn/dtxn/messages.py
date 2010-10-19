#!/usr/bin/python
# -*- coding: utf-8 -*-

from stupidcompiler import *

# This message can be used to add additional information about a transaction:
# TODO: Rename this to TransactionFragment?
# is it read-only? Is it multi-site? What communication is needed? A transaction id?
request = MessageDefinition("Fragment", "Contains a transaction fragment to be processed by the execution engine.")
# TODO: Add client ids so this id is unique across the entire DB?
request.addField(INT32, "client_id", 0, "Unique identifier for the client sending the transaction.")
request.addField(INT64, "id", 0, "Unique identifier for this transaction. Used to identify messages that belong to a single transaction.")
request.addField(BOOL, "multiple_partitions", False, "True if this transaction spans multiple partitions.")
request.addField(BOOL, "last_fragment", True, "True if this is the last fragment of this transaction for this partition.")
# TODO: Rename this to command? or fragment?
request.addField(STRING, "transaction", None, "The actual transaction to be processed.")
request.addField(STRING, "payload", None, "Fragment messsage payload")

response = MessageDefinition("FragmentResponse", "The response from the partition.")
response.addField(INT64, "id", 0, "Identifies the transaction that this decision refers to.")
# TODO: Change this to an enum
response.addField(INT32, "status", -1, "Contains the ExecutionEngine::Status for this transaction.")
# Rename this to output? Might be more clear.
response.addField(STRING, "result", None, "The result of the transaction.")
# TODO: The dependency id is always the previous transaction? Could be a bool?
response.addField(INT32, "dependency", -1, "ID of a previous transaction that this depends on.")

commit_decision = MessageDefinition("CommitDecision", "Instructs a replica to commit or abort a transaction.")
commit_decision.addField(INT32, "client_id", 0, "Unique identifier for the client sending the transaction.")
commit_decision.addField(INT64, "id", 0, "Identifies the transaction that this decision refers to.")
commit_decision.addField(BOOL, "commit", False, "True if the transaction should commit.")
commit_decision.addField(STRING, "payload", None, "Commit messsage payload")

log_entry = MessageDefinition("LogEntry", "Transaction record stored in a log.")
log_entry.addField(List(STRING), "fragments", None, "All fragments for the transaction collected together.")
log_entry.addField(BOOL, "multiple_partitions", False, "True if this transaction spans multiple partitions.")
log_entry.addField(INT32, "decision_log_entry", -1, "If this is a decision, this is the log entry for the transaction.")
log_entry.addField(BOOL, "commit", False, "If this is a decision, True if the transaction should be committed.")


if __name__ == "__main__":
    main([request, response, commit_decision, log_entry], "dtxn")
