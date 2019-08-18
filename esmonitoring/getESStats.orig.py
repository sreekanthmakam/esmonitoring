import getopt
import sys
import time
from datetime import datetime
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection


def main(argv):
    global es_host
    global es_port
    global es_protocol
    global es_secure
    global es_username
    global es_password
    global influxdb_url
    global es_url
    global delay, nodes_stats_before, nodes_stats_after

    delay = 10

    try:
        opts, args = getopt.getopt(argv, "h",
                                   ["es_host=", "es_port=", "es_protocol=", "es_host=", "influxdb_url=", "es_secure=",
                                    "es_username=", "es_password="])
    except getopt.GetoptError:
        print(
            'getESStats.py --es_protocol https --es_host 100.100.173.104 --es_port 9200 --es_secure=true --es_username=elastic --es_password qXDjQA8gUV --influxdb_url http://100.100.174.203:8086/write?db=ElasticSearch ')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(
                'getESStats.py --es_protocol https --es_host 100.100.173.104 --es_port 9200 --es_secure=true --es_username=elastic --es_password qXDjQA8gUV --influxdb_url http://100.100.174.203:8086/write?db=ElasticSearch')
            sys.exit()
        elif opt in "--es_host":
            es_host = arg
        elif opt in "--es_port":
            es_port = arg
        elif opt in "--es_protocol":
            es_protocol = arg
        elif opt in "--es_secure":
            es_secure = arg
        elif opt in "--es_username":
            es_username = arg
        elif opt in "--es_password":
            es_password = arg
        elif opt in "--influxdb_url":
            influxdb_url = arg

    es_url = es_protocol + '://' + es_username + ":" + es_password + "@" + es_host + ':' + es_port

    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: INPUT variables\n")
    print('\tES_HOST={es_host}\tES_PORT={es_port}\tES_PROTOCOL={es_protocol}\tES_SECURE={es_secure}\tES_USERNAME={es_username}\tES_PASSWORD={es_password}\tES_URL={es_url}\tINFLUXDB_URL={influxdb_url} \n\n\n')
    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: Start Collecting Cluster Wide Stats\n")

    get_cluster_stats()
    collect_stats_with_delay()
    measure_es_metrics()


def calculate_data(before, after, divby):
    if divby != 0:
        return ((after - before)/divby)
    else:
        return 0


def measure_es_metrics():
    for node_name in node_names:
        node = node_name['id']

        cluster_name = nodes_stats_before['cluster_name']
        node_hostname = nodes_stats_before['nodes'][node]['name']

        print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: Collecting ES Metrics for NODE = " + node)

        stats_time = calculate_data(nodes_stats_before['nodes'][node]['timestamp'], nodes_stats_after['nodes'][node]['timestamp'], 1000)

        indices_total_count = nodes_stats_before['nodes'][node]['indices']['docs']['count']
        indices_total_size = nodes_stats_before['nodes'][node]['indices']['store']['size_in_bytes']

        indexing_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['indexing']['index_total'],
                                      nodes_stats_after['nodes'][node]['indices']['indexing']['index_total'],
                                      stats_time)

        indexing_latency = calculate_data(nodes_stats_before['nodes'][node]['indices']['indexing']['index_time_in_millis'],
                                          nodes_stats_after['nodes'][node]['indices']['indexing']['index_time_in_millis'],
                                          indexing_ops)

        indexing_throttle_time = calculate_data(nodes_stats_before['nodes'][node]['indices']['indexing']['throttle_time_in_millis'],
                                                nodes_stats_after['nodes'][node]['indices']['indexing']['throttle_time_in_millis'],
                                                1)

        indexing_failed_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['indexing']['index_failed'],
                                             nodes_stats_after['nodes'][node]['indices']['indexing']['index_failed'],
                                             stats_time)

        indexing_delete_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['indexing']['delete_total'],
                                             nodes_stats_after['nodes'][node]['indices']['indexing']['delete_total'],
                                             stats_time)

        indexing_delete_latency = calculate_data(nodes_stats_before['nodes'][node]['indices']['indexing']['delete_time_in_millis'],
                                                 nodes_stats_after['nodes'][node]['indices']['indexing']['delete_time_in_millis'],
                                                 indexing_delete_ops)

        search_query_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['search']['query_total'],
                                          nodes_stats_after['nodes'][node]['indices']['search']['query_total'],
                                          stats_time)

        search_query_latency = calculate_data(nodes_stats_before['nodes'][node]['indices']['search']['query_time_in_millis'],
                                              nodes_stats_after['nodes'][node]['indices']['search']['query_time_in_millis'],
                                              search_query_ops)

        search_fetch_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['search']['fetch_total'],
                                          nodes_stats_after['nodes'][node]['indices']['search']['fetch_total'],
                                          stats_time)

        search_fetch_latency = calculate_data(nodes_stats_before['nodes'][node]['indices']['search']['fetch_time_in_millis'],
                                              nodes_stats_after['nodes'][node]['indices']['search']['fetch_time_in_millis'],
                                              search_fetch_ops)

        search_scroll_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['search']['scroll_total'],
                                           nodes_stats_after['nodes'][node]['indices']['search']['scroll_total'],
                                           stats_time)

        search_scroll_latency = calculate_data(nodes_stats_before['nodes'][node]['indices']['search']['scroll_time_in_millis'],
                                               nodes_stats_after['nodes'][node]['indices']['search']['scroll_time_in_millis'],
                                               search_scroll_ops)

        search_suggest_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['search']['suggest_total'],
                                            nodes_stats_after['nodes'][node]['indices']['search']['suggest_total'],
                                            stats_time)

        search_suggest_latency = calculate_data(nodes_stats_before['nodes'][node]['indices']['search']['suggest_time_in_millis'],
                                                nodes_stats_after['nodes'][node]['indices']['search']['suggest_time_in_millis'],
                                                search_suggest_ops)

        query_cache_hit_count = calculate_data(nodes_stats_before['nodes'][node]['indices']['query_cache']['hit_count'],
                                               nodes_stats_after['nodes'][node]['indices']['query_cache']['hit_count'],
                                               stats_time)

        query_cache_miss_count = calculate_data(nodes_stats_before['nodes'][node]['indices']['query_cache']['miss_count'],
                                                nodes_stats_after['nodes'][node]['indices']['query_cache']['miss_count'],
                                                stats_time)

        query_cache_cache_count = calculate_data(nodes_stats_before['nodes'][node]['indices']['query_cache']['cache_count'],
                                                 nodes_stats_after['nodes'][node]['indices']['query_cache']['cache_count'],
                                                 stats_time)

        query_cache_size = nodes_stats_after['nodes'][node]['indices']['query_cache']['memory_size_in_bytes']

        merge_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['merges']['total'],
                                   nodes_stats_after['nodes'][node]['indices']['merges']['total'],
                                   stats_time)

        merge_docs_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['merges']['total_docs'],
                                        nodes_stats_after['nodes'][node]['indices']['merges']['total_docs'],
                                        stats_time)

        merge_latency = calculate_data(nodes_stats_before['nodes'][node]['indices']['merges']['total_time_in_millis'],
                                       nodes_stats_after['nodes'][node]['indices']['merges']['total_time_in_millis'],
                                       merge_ops)

        merge_stopped_time = calculate_data(nodes_stats_before['nodes'][node]['indices']['merges']['total_stopped_time_in_millis'],
                                            nodes_stats_after['nodes'][node]['indices']['merges']['total_stopped_time_in_millis'],
                                            merge_ops)

        merge_throttled_time = calculate_data(nodes_stats_before['nodes'][node]['indices']['merges']['total_throttled_time_in_millis'],
                                              nodes_stats_after['nodes'][node]['indices']['merges']['total_throttled_time_in_millis'],
                                              merge_ops)

        refresh_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['refresh']['total'],
                                     nodes_stats_after['nodes'][node]['indices']['refresh']['total'],
                                     stats_time)

        refresh_latency = calculate_data(nodes_stats_before['nodes'][node]['indices']['refresh']['total_time_in_millis'],
                                         nodes_stats_after['nodes'][node]['indices']['refresh']['total_time_in_millis'],
                                         refresh_ops)

        flush_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['flush']['total'],
                                   nodes_stats_after['nodes'][node]['indices']['flush']['total'],
                                   stats_time)

        flush_latency = calculate_data(nodes_stats_before['nodes'][node]['indices']['flush']['total_time_in_millis'],
                                       nodes_stats_after['nodes'][node]['indices']['flush']['total_time_in_millis'],
                                       flush_ops)

        segments_count = nodes_stats_after['nodes'][node]['indices']['segments']['count']
        segments_memory = nodes_stats_after['nodes'][node]['indices']['segments']['memory_in_bytes']

        translog_ops = calculate_data(nodes_stats_before['nodes'][node]['indices']['translog']['operations'],
                                      nodes_stats_after['nodes'][node]['indices']['translog']['operations'],
                                      stats_time)

        influx_string = "ESMetrics,ClusterName=" + cluster_name + ",NodeName=" + node_hostname + \
                        " indices_total_count=" + str(indices_total_count) + \
                        ",indices_total_size=" + str(indices_total_size) + \
                        ",indexing_ops=" + str(indexing_ops) + \
                        ",indexing_latency=" + str(indexing_latency) + \
                        ",indexing_throttle_time=" + str(indexing_throttle_time) + \
                        ",indexing_delete_latency=" + str(indexing_delete_latency) + \
                        ",indexing_failed_ops=" + str(indexing_failed_ops) + \
                        ",indexing_delete_ops=" + str(indexing_delete_ops) + \
                        ",search_query_ops=" + str(search_query_ops) + \
                        ",search_fetch_ops=" + str(search_fetch_ops) + \
                        ",search_scroll_ops=" + str(search_scroll_ops) + \
                        ",search_suggest_ops=" + str(search_suggest_ops) + \
                        ",search_query_latency=" + str(search_query_latency) + \
                        ",search_fetch_latency=" + str(search_fetch_latency) + \
                        ",search_scroll_latency=" + str(search_scroll_latency) + \
                        ",search_suggest_latency=" + str(search_suggest_latency) + \
                        ",query_cache_hit_count=" + str(query_cache_hit_count) + \
                        ",query_cache_miss_count=" + str(query_cache_miss_count) + \
                        ",query_cache_cache_count=" + str(query_cache_cache_count) + \
                        ",query_cache_size=" + str(query_cache_size) + \
                        ",refresh_ops=" + str(refresh_ops) + \
                        ",refresh_latency=" + str(refresh_latency) + \
                        ",merge_ops=" + str(merge_ops) + \
                        ",merge_docs_ops=" + str(merge_docs_ops) + \
                        ",merge_latency=" + str(merge_latency) + \
                        ",merge_stopped_time=" + str(merge_stopped_time) + \
                        ",merge_throttled_time=" + str(merge_throttled_time) + \
                        ",flush_ops=" + str(flush_ops) + \
                        ",flush_latency=" + str(flush_latency) + \
                        ",segments_count=" + str(segments_count) + \
                        ",segments_memory=" + str(segments_memory) + \
                        ",translog_ops=" + str(translog_ops)

        print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: uploading ES Stats to INFLUX")
        print(influx_string)
        resp = requests.post(influxdb_url, data=influx_string)
        print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: RESPONSE FROM INFLUX = " + str(
            resp.status_code))


def collect_stats_with_delay():

    global nodes_stats_before
    global nodes_stats_after

    es = Elasticsearch(es_url, verify_certs=False, connection_class=RequestsHttpConnection)
    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: Fetch Node Stats Before\n")
    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: SLEEP for " + str(delay) + " secs \n")
    nodes_stats_before = es.nodes.stats()

    time.sleep(delay)

    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: Fetch Node Stats After Delay=" + str(delay) + "secs\n")
    nodes_stats_after = es.nodes.stats()


def get_cluster_stats():
    global node_names
    global header_string
    global ClusterName

    payload = {'format': 'json'}
    header_string = {"Content-Type": "application/json"}

    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: Fetch Node Names\n")
    try:
        resp = requests.request('GET', es_url + '/_cat/nodes?full_id&v&h=id,ip,node.role',
                                params=payload,
                                headers=header_string,
                                verify=False)
        node_names = resp.json()
    except requests.exceptions.RequestException as e:
        print(e)
        print("[ERROR] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') +"]: SOME ISSUE IN CONNECTING TO ES SERVER. EXITING!!!!!!")
        sys.exit(1)

    # Count master nodes
    number_of_master_nodes = 0
    number_of_ingest_nodes = 0
    number_of_master_data_nodes = 0
    for node_name in node_names:
        print("\t\t" + node_name['id'] + "\t" + node_name['ip'] + "\n")
        if "m" in node_name['node.role']:
            number_of_master_nodes += 1
            master_node = True
        if "d" in node_name['node.role']:
            data_node = True
        if "i" in node_name['node.role']:
            number_of_ingest_nodes += 1
        if master_node and data_node:
            number_of_master_data_nodes += 1

    # Fetch all stats
    es = Elasticsearch(es_url, verify_certs=False, connection_class=RequestsHttpConnection)

    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: Fetch Cluster Health\n")
    cluster_health = es.cluster.health()
    indices_cat = es.cat.indices(format="json", bytes="b")

    total_indices = 0
    total_green_indices = 0
    total_open_indices = 0
    total_docs = 0
    total_size = 0
    for index in indices_cat:
        total_indices += 1
        total_docs += int(index["docs.count"])
        total_size += int(index["store.size"])
        if index["health"] == "green":
            total_green_indices += 1
        if index["status"] == "open":
            total_open_indices += 1

    total_non_green_indices = total_indices - total_green_indices
    total_closed_indices = total_indices - total_green_indices

    # Fetch Cluster health
    ClusterName = cluster_health['cluster_name']
    cluster_status = cluster_health['status']
    if cluster_status == 'green':
        ClusterStatus_num = 100
    elif cluster_status == 'red':
        ClusterStatus_num = 0
    elif cluster_status == 'yellow':
        ClusterStatus_num = 50
    else:
        ClusterStatus_num = 100

    number_of_data_nodes = cluster_health['number_of_data_nodes']
    number_of_nodes = cluster_health['number_of_nodes']
    active_primary_shards = cluster_health['active_primary_shards']
    active_shards = cluster_health['active_shards']
    active_shards_percent_as_number = cluster_health['active_shards_percent_as_number']

    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: Below is Cluster Wide Stats Collected\n")
    data = "ClusterStats,ClusterName=" + ClusterName + \
           " ClusterStatus=" + str(ClusterStatus_num) + \
           ",number_of_nodes=" + str(number_of_nodes) + \
           ",number_of_data_nodes=" + str(number_of_data_nodes) + \
           ",number_of_master_nodes=" + str(number_of_master_nodes) + \
           ",active_primary_shards=" + str(active_primary_shards) + \
           ",active_shards=" + str(active_shards) + \
           ",active_shards_percent_as_number=" + str(active_shards_percent_as_number) + \
           ",total_index=" + str(total_indices) + \
           ",total_healthy_index=" + str(total_green_indices) + \
           ",total_non_green_indices=" + str(total_non_green_indices) + \
           ",total_open_indices=" + str(total_open_indices) + \
           ",total_closed_indices=" + str(total_closed_indices) + \
           ",total_docs=" + str(total_docs) + \
           ",total_size=" + str(total_size)

    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: uploading Cluster Stats to INFLUX")
    print(data)
    resp = requests.post(influxdb_url, data=data)
    print("[INFO] [" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]: RESPONSE FROM INFLUX = " + str(resp.content))


if __name__ == "__main__":

    main(sys.argv[1:])
