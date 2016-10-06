# calculate LDA with spark mllib from a dataset stored on elasticsearch 

## build

sbt package

## execute

```bash
calculate LDA with data from an elasticsearch index.
Usage: LDA with ES data [options]

  --hostname <value>       the hostname of the elasticsearch instance  default: localhost
  --port <value>           the port of the elasticsearch instance  default: 9200
  --group_by_field <value>
                           group the search results by field e.g. conversation, None => no grouping  default: None
  --search_path <value>    the search path on elasticsearch e.g. <index name>/<type name>  default: jenny-en-0/question
  --query <value>          a json string with the query  default: { "fields":["question", "answer", "conversation",
                                                                               "index_in_conversation", "_id" ] }
  --min_k <value>          min number of topics. default: 8
  --max_k <value>          max number of topics. default: 10
  --maxTermsPerTopic <value>
                           the max number of terms per topic. default: 10
  --maxIterations <value>  number of iterations of learning. default: 100
  --stopwordFile <value>   filepath for a list of stopwords. Note: This must fit on a single machine.  default: Some(stopwords/en_stopwords.txt)
  --used_fields <value>    list of fields to use for LDA, if more than one they will be merged  default: List(question, answer)
```

## run with spark

```bash
./scripts/run.sh [Options]
```