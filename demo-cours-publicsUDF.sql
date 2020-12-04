- 3 UDF PUBLIQUE
--
SELECT bqutil.fn.int(1.684)
;
SELECT bqutil.fn.nlp_compromise_people(
  "hello, I'm Felipe Hoffa and I work with Elliott Brossard - who thinks Jordan Tigani will like this post?"
  ) names
;
SELECT bqutil.fn.nlp_compromise_people(
  "Donald Duck ? Kylian Mbappé xxx Emmanuel macron xxx donald trump"
  ) names
;
WITH urls AS (
  SELECT 'http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' as url
  UNION ALL
  SELECT 'rpc://facebook.com/' as url
)
SELECT bqutil.fn.url_parse(url, 'HOST'), bqutil.fn.url_parse(url, 'PATH'), bqutil.fn.url_parse(url, 'QUERY'), bqutil.fn.url_parse(url, 'REF'), bqutil.fn.url_parse(url, 'PROTOCOL') from urls
;