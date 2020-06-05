curl -H "Content-Type: application/x-ndjson" -XPOST -u username:password "http://localhost:9200/sample/data/_bulk?pretty&refresh" --data-binary @"sample.json"

sample.json

{"index" : {}}
{"name":"test1", "id":1}
{"index" : {}}
{"name":"test2", "id":2}
{"index" : {}}
{"name":"test3", "id":3}
{"index" : {}}
{"name":"test4", "id":4}
