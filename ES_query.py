from elasticsearch import Elasticsearch

# connect to ES cluster
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

e1 = {
    'first_name': 'nitin',
    'last_name': 'panwar',
    'age': 27,
    'about': 'Love to play cricket',
    'interests': ['sports', 'music']
}

e2={
    "first_name":  "Jane",
    "last_name":   "Smith",
    "age":         32,
    "about":       "I like to collect rock albums",
    "interests":  ["music"]
}

e3={
    "first_name":  "Douglas",
    "last_name":   "Fir",
    "age":         35,
    "about":        "I like to build cabinets",
    "interests":  ["forestry"]
}

e4={
    "first_name":"asd",
    "last_name":"pafdfd",
    "age": 27,
    "about": "Love to play football",
    "interests": ['sports','music'],
}

# print(e1)

# Store document in ES
res = es.index(index='megacorp', doc_type='employee', id=1, body=e1)
res = es.index(index='megacorp', doc_type='employee', id=2, body=e2)
res = es.index(index='megacorp', doc_type='employee', id=3, body=e3)
res = es.index(index='megacorp', doc_type='employee', id=4, body=e4)


# print(res)

# Searching


# query_body = {
#     'query': {
#         'match': {
#             'about': 'play cricket'
#         }
#     }
# }

query_body = {
    'aggs': {
        'all_interests': {
            'terms': {
                'field': 'interests.keyword'
            }
        }
    }
}

res = es.search(index='megacorp', body=query_body)
#
# for hit in res['hits']['hits']:
#     print(hit['_source']['about'])
#     print(hit['_score'])
#     print("*************")

for interest in res['aggregations']['all_interests']['buckets']:
    print('{} interest have {} documents'.format(interest['key'], interest['doc_count']))
    print("****************")