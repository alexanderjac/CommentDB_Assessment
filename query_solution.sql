

-- 1st question:
	SELECT DATE(comment_info.created_time) AS comment_date, COUNT(comment_text.h_id) AS top_level_comments_count
FROM comment_info
JOIN comment_text ON comment_info.comment_id = comment_text.h_id
WHERE comment_info.comment_count = 0
GROUP BY comment_date
ORDER BY top_level_comments_count DESC
LIMIT 1;
/* 
Solution:
+--------------+--------------------------+
| comment_date | top_level_comments_count |
+--------------+--------------------------+
| 2018-01-10   |                     1903 |
+--------------+--------------------------+
*/


-- 2nd question:

    SELECT
    	p.type AS post_type, count(ci.comment_id) AS comment_count, sum(ci.like_count) AS comment_up_likes 
    FROM
    	comment_info ci
    JOIN
    	post_meta p on ci.post_id = p.post_h_id
    WHERE
    	ci.created_time > '2018-01-10' 
    GROUP BY
    	p.type
    ORDER BY
    	p.type;

/*
Solution:
+-----------+---------------+------------------+
| post_type | comment_count | comment_up_likes |
+-----------+---------------+------------------+
| l         |            85 |               29 |
| p         |          1405 |             1024 |
| s         |           552 |             1463 |
| v         |         70114 |            69121 |
+-----------+---------------+------------------+
 */


-- 3rd question:

SELECT
    p.post_h_id AS page_id,
    p.name_h_id AS page_name,
    AVG(LENGTH(ct.message)) AS avg_comment_length
FROM
    comment_text ct
JOIN
    comment_info ci ON ct.h_id = ci.comment_id
JOIN
    post_meta p ON ci.post_id = p.post_h_id
GROUP BY
    p.post_h_id, p.name_h_id
ORDER BY
    avg_comment_length DESC
LIMIT 5;

/*
Solution:
+---------------------------------------------+----------------------------------------------+--------------------+
| page_id                                     | page_name                                    | avg_comment_length |
+---------------------------------------------+----------------------------------------------+--------------------+
| NjEyNDAwOTAyMV8xMDE1NTU4OTQxOTc4NDAyMg      | VFJM                                         |           179.2000 |
| NjEyNDAwOTAyMV8xMDE1NTU0ODUxMTY5NDAyMg      | VFJM                                         |           155.5294 |
| NjEyNDAwOTAyMV8xMDE1NTU0ODQ4MjkwOTAyMg      | VFJM                                         |           124.7500 |
| MTE0MDQzMTc1MzUyMDI3XzE2MTI2MDQ5Mzg4MjkxNjk | TmljayBDYW5ub24gUHJlc2VudHM6IFdpbGQgJ04gT3V0 |           113.3820 |
| NjEyNDAwOTAyMV8xMDE1NTU0ODQ4NzU3OTAyMg      | VFJM                                         |           105.3945 |
+---------------------------------------------+----------------------------------------------+--------------------+
 */

-- 4th question:


SELECT pm.name_h_id AS page_name,
       ci.post_id AS post_id,
       ci.comment_id AS comment_id,
       COUNT(cr.comment_id) AS num_replies,
       ct.message AS top_comment_text
FROM comment_info ci
JOIN post_meta pm ON ci.post_id = pm.post_h_id
JOIN comment_text ct ON ci.comment_id = ct.h_id
JOIN comment_info cr ON ct.h_id = cr.comment_id
WHERE ci.comment_count > 0 -- only consider top level comments with replies
GROUP BY page_name, post_id, comment_id, top_comment_text
ORDER BY num_replies DESC
LIMIT 5;

/*
Solution:
+----------------------------------------------+---------------------------------------------+----------------------------------------------+-------------+-----------------------------------------------------------------------+
| page_name                                    | post_id                                     | comment_id                                   | num_replies | top_comment_text                                                      |
+----------------------------------------------+---------------------------------------------+----------------------------------------------+-------------+-----------------------------------------------------------------------+
| TmljayBDYW5ub24gUHJlc2VudHM6IFdpbGQgJ04gT3V0 | MTE0MDQzMTc1MzUyMDI3XzE1OTE2NDY5Nzc1OTE2MzI | MTU5MTY0Njk3NzU5MTYzMl8xNTkxNjU4MTUwOTIzODQ4 |         324 | xhx  wxdx lyxcx hw xk x lxg x wxx xhm                                 |
| TmljayBDYW5ub24gUHJlc2VudHM6IFdpbGQgJ04gT3V0 | MTE0MDQzMTc1MzUyMDI3XzE1OTE2NDY5Nzc1OTE2MzI | MTU5MTY0Njk3NzU5MTYzMl8xNTkxNzk4MjUwOTA5ODM4 |         324 | lkxg xh mxx fxm x Gffxy gxff why xck lxh xyx lk x my 🦒 Chyxx lxg x     |
| TmljayBDYW5ub24gUHJlc2VudHM6IFdpbGQgJ04gT3V0 | MTE0MDQzMTc1MzUyMDI3XzE1OTE2NDY5Nzc1OTE2MzI | MTU5MTY0Njk3NzU5MTYzMl8xNTkyMzczOTQ0MTg1NjAy |         216 | pxx  gxlx  x xh gx                                                    |
| TmljayBDYW5ub24gUHJlc2VudHM6IFdpbGQgJ04gT3V0 | MTE0MDQzMTc1MzUyMDI3XzE1OTk5MjYzNzY3NjM2OTI | MTU5OTkyNjM3Njc2MzY5Ml8xNTk5OTU1NDczNDI3NDQ5 |         189 | fxm Cm x fvxx xppx x xxL xh ll Bxck bck chxc my                       |
| TmljayBDYW5ub24gUHJlc2VudHM6IFdpbGQgJ04gT3V0 | MTE0MDQzMTc1MzUyMDI3XzE1OTk5MjYzNzY3NjM2OTI | MTU5OTkyNjM3Njc2MzY5Ml8xNTk5OTc2MDAzNDI1Mzk2 |         126 | 🤔 hxxbl  Bx wx bm                                                      |
+----------------------------------------------+---------------------------------------------+----------------------------------------------+-------------+-----------------------------------------------------------------------+
 */