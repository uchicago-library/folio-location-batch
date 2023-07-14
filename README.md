# folio-utils
A set of utilities for automating some tasks in FOLIO

## Requirements

Install requirements through pip:

```
python -m pip install -r requirements.txt
```

## Utilities

### pol_expenseclasses.py

Change the expense classes on fund distributions in POLs.

Used when we were reorganizing budget expense classes in preparation for change in University budget practices.

### pol_fund.py

Update funds in purchase order lines.

Used annually to reassign funds on open orders.

### pol_reencumber.py

Re-encumber funds on purchase order lines.

Used when rollover failed to encumber funds in new fiscal year for open orders.