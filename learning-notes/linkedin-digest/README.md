# Data engineering learning notes

Short, original notes and practice exercises prompted by [Shubham Srivastava’s LinkedIn posts](https://www.linkedin.com/in/shubham-srivstv/recent-activity/all/). Daily entries with new, verified posts go in `YYYY-MM-DD.md` files in this directory.

## Start here: make a CDC merge safe to replay

Shubham’s [post about 25 data engineering patterns](https://www.linkedin.com/posts/shubham-srivstv_if-you-want-to-get-started-with-data-engineering-share-7530617959598452737-OrUU/) connects incremental ingestion, upserts, idempotency, and data quality. Start by testing how those patterns interact when source events arrive out of order.

1. Create synthetic changes keyed by `order_id`. Give each change a stable source sequence number and include a duplicate event, a stale event, and a delete.
2. Reduce the batch to the highest sequence per key, then merge into a Delta table. Apply a change only when its sequence exceeds the stored sequence. Keep a tombstone for deletes so that a late update cannot resurrect an order.
3. Replay the same batch. Assert that the active rows and stored sequences are identical after the replay. Then send an older update for the deleted order and verify it remains deleted.

**Interview prompt:** How long must you retain delete tombstones if the pipeline can replay historical events, and what happens when the retention window is too short?
