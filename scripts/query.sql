-- TABLES:
-- bh (blockID, hash, block_timestamp, n_txs)
-- addresses (addrID, address)
-- tx (txID, blockID, n_inputs, n_outputs)
-- txin (txID, input_seq, prev_txID, prev_output_seq, addrID, sum)
-- txout (txID, output_seq, addrID, sum)
-- addr_sccs (addrID, userID)

-- TO CREATE A USER DATASET
select
  addresses.addres,
  addr_sccs.userID,
  sum(incoming.sum) as total_received,
  sum(outgoing.sum) as total_spent,
  count(created_by_me) as issued_transactions,
  count(incoming) as received_transactions,
  max(outgoing.sum) as max_issued,
  min(outgoing.sum) as min_issued
from
  addresses
  join addr_sccs on addr_sccs.addrID = addresses.addrID
  join txout as incoming on addresses.addrID = incoming.addrID
  join txin as tmp on addresses.addrID = tmp.addrID
  join tx as created_by_me on tmp.txID = created_by_me.txID
  join txout as outgoing on created_by_me.txID = outgoing.txID
group by
  addrID

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
where
  bh.blockID between 250000
  and 300000
group BY
  txID
