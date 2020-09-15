Reforged copy of script https://github.com/trevorndodds/elasticsearch-metrics/blob/master/Grafana/elasticsearch2elastic.py

For Grafana Dashbord https://grafana.com/grafana/dashboards/878

Used config file
Write all event's to log


Prepare to collect metrics:

PUT /_template/elasticsearch_metrics
{
    "index_patterns" : [
      "elasticsearch_metrics-*"
    ],
    "settings" : {
      "index" : {
        "mapping.total_fields.limit": 2000,
        "number_of_shards" : "1",
        "auto_expand_replicas" : null,
        "number_of_replicas" : "1"
      }
    }
  }
