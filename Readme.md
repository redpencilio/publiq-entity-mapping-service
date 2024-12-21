# Entity Mapping Service

Query for listing mappings:
```sparql
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX locn: <http://www.w3.org/ns/locn#>
PREFIX sssom: <https://w3id.org/sssom/>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT ?created ?sAddress ?subject_label ?oAddress ?object_label ?score
WHERE {
    ?mapping a sssom:Mapping ;
        dct:created ?created ;
        sssom:subject_id ?subject ;
        sssom:subject_label ?subject_label ;
        sssom:object_id ?object ;
        sssom:object_label ?object_label ;
        sssom:similarity_score ?score .
    ?subject locn:fullAddress ?sAddress .
    ?object locn:fullAddress ?oAddress .
}
ORDER BY DESC(?created)
LIMIT 100
```
