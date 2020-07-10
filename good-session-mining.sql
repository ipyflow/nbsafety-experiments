SELECT t1.* from cell_execs t1
INNER JOIN (
    SELECT DISTINCT trace, session
    FROM cell_execs
    WHERE source LIKE '%sklearn.datasets%'
        EXCEPT
    SELECT *
    FROM (
             SELECT *
             FROM good_sessions
             UNION
             SELECT *
             FROM bad_sessions
         )
) t2
ON t1.trace = t2.trace AND t1.session = t2.session
--       AND t1.trace > 311
ORDER BY t1.trace ASC, t1.session ASC, t1.counter ASC
