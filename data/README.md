# Synthetic order data

[`order_changes.json`](order_changes.json) contains the complete seed for the local Bronze-to-Silver repair example. All orders and changes are synthetic.

## Fields

| Key | Contents |
| --- | --- |
| `note` | Descriptive metadata; the loader does not use it. |
| `batches` | Three lists of order changes. Each list is appended to Bronze as one commit, so Bronze ends at version 2. |
| `arrives_after_pin` | One more change, for order 8, that lands after the repair pins Bronze version 2. |

Each change contains `order_id`, `seq`, `order_date`, and `raw_status`. `seq` is the source's own order of changes for one order; the highest `seq` wins. Dates are ISO `YYYY-MM-DD` strings. `raw_status` is `PLACED`, `SHIPPED`, or `CANCELLED`.

## Scenario order

1. Append the three batches to Bronze and build Silver and Gold with the faulty rule (`CANCELLED` becomes `shipped`).
2. Rerun the job with the corrected rule, then let order 8 arrive and run again.
3. Repair the date range 3 to 5 November from Bronze version 2.
4. Repeat that repair, and try to write a row outside its scope.
5. Compare Silver with a full rebuild from the latest Bronze version.
6. Repair by order keys 2, 5, and 7.
7. Repair the affected Gold dates.
8. Vacuum Silver with no retention and try to read the pre-repair version.

Order 7 is the interesting one. Its order date is 1 November, but its cancellation arrives in the last batch, so a repair scoped to 3 to 5 November misses it.
