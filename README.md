# PyMysqlSlowLog #
### reading mysql slow log with Pandas ###


## Example of usage ##

```
import pandas as pd
import mysqlslowlog

df = mysqlslowlog.read('slowlog/mariadb-slow.log',save_sql=False)

print(df.head(5))
print(df.dtypes)
```
or [View example jupter notebook](bytesort_investigation_example.ipynb)

## Other programs that will help in the analysis of slow mysql queries ##

[pt-query-digest](https://www.percona.com/doc/percona-toolkit/LATEST/pt-query-digest.html) - it's a pretty old program. Unlike this module, it can group requests by similarity.