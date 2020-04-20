-- TABLES:
-- bh (blockID, hash, block_timestamp, n_txs)
-- addresses (addrID, address)
-- tx (txID, blockID, n_inputs, n_outputs)
-- txin (txID, input_seq, prev_txID, prev_output_seq, addrID, sum)
-- txout (txID, output_seq, addrID, sum)
-- addr_sccs (addrID, userID)

-- TO CREATE A USER DATASET WITH ALL USERS
select
  all_users.address_id,
  sum(incoming.sum) as total_received,
  sum(outgoing.sum) as total_spent,
  count(created_by_me.id) as issued_transactions,
  count(incoming.id) as received_transactions,
  max(outgoing.sum) as max_issued,
  min(outgoing.sum) as min_issued
from
  (SELECT DISTINCT address_id from txin UNION SELECT DISTINCT address_id FROM txout) as all_users
  join txout as incoming on incoming.address_id = all_users.address_id
  join txin as tmp on all_users.address_id = tmp.address_id
  join tx as created_by_me on tmp.id = created_by_me.id
  join txout as outgoing on created_by_me.id = outgoing.id
group by
  all_users.address_id

-- TO CREATE A USER DATASET WITH ONLY THE USERS WHO RECEIVED TXS
select
  all_users.address_id,
  sum(incoming.sum) as total_received,
  sum(outgoing.sum) as total_spent,
  count(created_by_me.id) as issued_transactions,
  count(incoming.id) as received_transactions,
  max(outgoing.sum) as max_issued,
  min(outgoing.sum) as min_issued
from
  txout
  join txout as incoming on incoming.address_id = all_users.address_id
  join txin as tmp on all_users.address_id = tmp.address_id
  join tx as created_by_me on tmp.id = created_by_me.id
  join txout as outgoing on created_by_me.id = outgoing.id
group by
  all_users.address_id

-- TO CREATE A TXS DATASET
select
  tx.txID as id,
  bh.block_timestamp as timestamp,
  addresses.address as src_addr,
  tx.n_inputs,
  tx.n_outputs,
  sum(txin.sum) as inputs_sum,
  sum(txout.sum) as outputs_sum,
  min(txin.sum) as min_input,
  max(txin.sum) as max_input,
  min(txout.sum) as min_ouput,
  max(txout.sum) as max_output
from
  tx
  join bh on tx.blockID = bh.blockID
  join txin on tx.txID = txin.txID
  join txout on tx.txID = txout.txID
  join addresses on txin.addrID = addresses.addrID
group BY
  tx.txID
