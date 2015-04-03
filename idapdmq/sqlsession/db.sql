SELECT *
FROM trans_journal;


SELECT count(*)
FROM trans_journal;
SELECT *
FROM trans_journal
ORDER BY trans_date ASC;
SELECT *
FROM trans_journal
ORDER BY seq DESC;

SELECT * from channel;

SELECT DISTINCT channel FROM trans_journal;

SELECT
  channel,
  count(channel)                          cnt,
  sum(to_number(amount, '9999999999D9S')) amt
FROM trans_journal
GROUP BY channel
ORDER BY amt DESC;
SELECT
  branch,
  count(branch)                           cnt,
  sum(to_number(amount, '9999999999D9S')) amt
FROM trans_journal
GROUP BY branch
ORDER BY amt DESC;
SELECT
  acct,
  count(acct)                             cnt,
  sum(to_number(amount, '9999999999D9S')) amt
FROM trans_journal
GROUP BY acct
ORDER BY amt DESC;

SELECT * FROM acct_balance;

SELECT
  period,
  count(period),
  sum(to_number(amount, '9999999999D9S')) amt
FROM (SELECT
        (substr(trans_date, 12, 2) || ':00') period,
        *
      FROM trans_journal
      WHERE trans_date >= '2015-01-02 08:00:00' AND trans_date <= '2015-01-02 18:00:00') t
GROUP BY period
ORDER BY period;

SELECT count(*)
FROM acct_balance;
SELECT *
FROM acct_balance;

SELECT
  j.*,
  b.balance
FROM acct_balance b,
  (SELECT
     acct,
     count(acct)                             trans_cnt,
     sum(to_number(amount, '9999999999D9S')) trans_amt
   FROM trans_journal
   GROUP BY acct) j
WHERE b.acct = j.acct;

SELECT *
FROM t_ccy;

SELECT
  a.*,
  to_number(a.amount, '9999999999D9S') * b.exch_rate amt_cny,
  b.exch_rate
FROM trans_journal a, t_ccy b
WHERE a.ccy = b.ccy;


SELECT
  t.*,
  t.sum_amt_cny + b.balance cur_bal
FROM acct_balance b, (SELECT
                        a.acct,
                        count(acct) cnt,
                        sum(to_number(a.amount, '9999999999D9S') * b.exch_rate) sum_amt_cny
                      FROM trans_journal a, t_ccy b
                      WHERE a.ccy = b.ccy
                      GROUP BY a.acct) t
WHERE b.acct = t.acct
ORDER BY cur_bal DESC;

UPDATE t_ccy
SET exch_rate = random() * 10;

SELECT count(*)
FROM (SELECT pg_stat_get_backend_idset() backendid) s;

SELECT DISTINCT acct
FROM trans_journal;
SELECT DISTINCT acct
FROM trans_journal;
INSERT INTO acct_balance (acct) SELECT DISTINCT acct
                                FROM trans_journal;
INSERT INTO t_ccy (ccy) SELECT DISTINCT ccy
                        FROM trans_journal;

CREATE TABLE channel AS SELECT DISTINCT channel FROM trans_journal;
CREATE TABLE branch AS SELECT DISTINCT branch FROM trans_journal;
SELECT DISTINCT ccy
INTO t_ccy
FROM trans_journal;

TRUNCATE trans_journal;
TRUNCATE acct_balance;
TRUNCATE t_ccy;

CREATE INDEX idx_trans_date ON trans_journal USING BTREE (trans_date);
CREATE INDEX idx_channel ON trans_journal USING BTREE (channel);
DROP INDEX idx_channel;

COPY (SELECT *
      FROM trans_journal) TO '/var/lib/postgres/demo_public_trans_journal_data_3M';
COPY (SELECT *
      FROM acct_balance) TO '/var/lib/postgres/demo_public_acct_balance_data';
COPY (SELECT *
      FROM t_ccy) TO '/var/lib/postgres/demo_public_t_ccy_data';

UPDATE acct_balance
SET balance = random() * 50000;
UPDATE t_ccy
SET exch_rate = random() * 10;


UPDATE acct_balance SET trans_cnt = 0;
UPDATE branch SET trans_cnt = 0;
UPDATE channel SET trans_cnt = 0;
