
1   old_pr = load 'aa.csv' USING PigStorage(',') as (influencer_book:int, influenced_book:int, sc:float);
2   scdata1 = group scdata by $0;
3   scdata1 = group scdata by ($0,$1);
4   scdata1 = group scdata by $0;
5   agroup = group scdata by scdata.influenced_book;
6   daily    = load 'NYSE_daily' as (exchange, symbol);
7   grpd     = group daily by exchange;
8   daily = load 'NYSE_daily' as (exchange, symbol);
9   daily = load 'NYSE_daily.txt' as (exchange, symbol);
10   grpd     = group daily by exchange;
11   uniqcnt  = foreach grpd {
           sym      = daily.symbol;
           uniq_sym = distinct sym;
           generate group, COUNT(uniq_sym);
};
12   agroup = group scdata by $1;
13   agcnt = foreach agroup generate group as infid, COUNT(scdata) as cnt;
14   x =  filter agcnt by $0 == 39
;
15   x1 = foreach x generate x.cnt;
16   scdata1for = foreach scdata1 generate group as iid, scdata.influenced_book;
17   scdata1for = foreach scdata1 generate group as iid, scdata.influenced_book as bid;
18   scdata1for1 = foreach scdata1for generate iid, FLATTEN(BagToTuple(bid.influenced_book));
19   scdata1for = foreach scdata1 generate group as iid, scdata.influenced_book as bid;
20   scdata1for = foreach scdata1 generate group as iid, scdata.influenced_book;
21   scdata1for = foreach scdata1 generate group as iid, scdata.influenced_book as bid;
22   scdata = load 'HW4-book_ranks.csv' USING PigStorage(',') as (influencer_book:int, page_rank:float);
23   scdata = load 'aa.csv' USING PigStorage(',') as (influencer_book:int, influenced_book:int, sc:float);
24   oldpr = load 'HW4-book_ranks.csv' USING PigStorage(',') as (influencer_book:int, page_rank:float);
25   oldpr = load 'HW4-book_ranks.csv' USING PigStorage(',') as (influencer_book:int, page_rank:float);
26   oldpr = load 'HW4-book_ranks.csv' USING PigStorage('\t') as (influencer_book:int, page_rank:float);
27   oldsc = join scdata1for by $0, oldpr by $0;
28   oldsc1 = foreach oldsc generate $0,$1,$3;
29   oldsc1 = foreach oldsc generate $0 as iid,$1 as links,$3 as olr;
30   opr = foreach oldsc1 generate olr/COUNT(links) as npr, FLATTEN(links) as to_url;
31   npr1 = foreach (COGROUP opr by to_url, oldsc1 by iid) generate group as url, ((1-0.85)/50)+0.85*SUM(opr.npr) as npr, FLATTEN(oldsc1.links) as links;

#book_rank_iteration.pig

old_pr = load 'aa.csv' USING PigStorage(',') as (influencer_book:int, influenced_book:int, sc:float);
old_pr = foreach old_pr generate $0, $1;
oldbr = load 'HW4-book_ranks.csv' USING PigStorage('\t') as (influencer_book:int, page_rank:float);
oldpr_gb2 = group old_pr by $1;
oldpr_gb3 = foreach oldpr_gb2 generate group as influenced_book, COUNT(old_pr) as n;
oldbr_join = join oldpr_gb3 by $0, oldbr by $0;
oldbr_join1 = foreach oldbr_join generate $0 as influenced_book, $1 as n, $3 as opr;
oldbr_pr = join old_pr by $1, oldbr_join1 by $0;
oldbr_pr1 = foreach oldbr_pr generate $0 as url, $1 as link, $3 as n, $4 as pr;
oldbr_pr2 = foreach oldbr_pr1 generate $0, pr/n as npr;
oldbr_pr3 = foreach (COGROUP oldbr_pr2 by url, oldbr by influencer_book INNER) generate group as book_id, ((1-0.85)/50) + 0.85* SUM(oldbr_pr2.npr) as pagerank;
store oldbr_pr3 into 'abc6' using PigStorage('\t','-schema');

hadoop fs -getmerge -nl abc6 HW4-book_ranks.csv

#to get the top percentile ranks
In this task you will implement a Pig Latin script named find_ k_percentile_books.pig to find the book_ids, book_titles and authors of the k-percentile books based on their book_ranks where k is a user input to the query.

data = load 'HW4-book_ranks.csv' using PigStorage(',') as (book_id:int,page_rank:float);
book = LOAD '/opt/mongo/bodi0000/book.csv' USING PigStorage(',') AS(book_id:int,book_title:chararray,author_id:int,release_date:int);
book_d =  join book by $0, data by $0;
book_d1 = foreach book_d generate $0 as book_id, $1 as book_title, $2 as author_id, $5 as page_rank;
book_d2 = order book_d1 by book_id DESC;
book_d3= limit book_d2 (int)($input * 50 /100);
book_d4 = foreach book_d3 generate $0,$1,$2;
book_d5 = group book_d4 all;
book_d6 = foreach book_d5 generate $1;
dump book_d6;
store book_d6 into 'query_5';
