#!/usr/bin/python
import numpy as np
from collections import Counter
import sys

imp_map = {
    'Dev': 'low', 'Admin': 'high', 'Mgmt': 'high',
    'IT': 'high', 'HR': 'medium',
    'SkypeEmail': 'low', 'CloudServices': 'low',
    'Computing': 'low', 'Infra': 'high', 'Ext': 'low'
}

vul_map = {
    'Dev': 'low', 'Admin': 'medium', 'Mgmt': 'medium',
    'IT': 'low', 'HR': 'medium',
    'SkypeEmail': 'low', 'CloudServices': 'low',
    'Computing': 'low', 'Infra': 'low', 'Ext': 'low'
}


def get_machines(machine_types, p_machines, num_machines):
    machine_type_list = np.random.choice(machine_types, num_machines, p=p_machines)
    machine_list = [{'type': x} for x in machine_type_list]
    machine_list = [{'type': x, 'imp': imp_map[x], 'vul': vul_map[x]} for x in machine_type_list]
    return machine_list


def get_machine_map(machine_list):
    machine_map = dict()
    for m in machine_list:
        if m['type'] not in machine_map:
            machine_map[m['type']] = []
        machine_map[m['type']].append(m)
    return machine_map


def make_flow(timestamp, src, dst, proto, data_vol):
    flow = {'timestamp': timestamp, 'src': src, 'dst': dst, 'proto': proto, 'data_vol': data_vol}
    return flow


def add_node_ids(machine_list):
    machine_id = 0
    for m in machine_list:
        m['id'] = m['type'] + '_' + str(machine_id)
        machine_id += 1
    return machine_list


def store_nodes(machines, out_prefix):
    outpath = out_prefix + '.nodes.txt'
    print('storing node -> ' + outpath)
    f_nodes = open(outpath, 'w')
    for m in machines:
        f_nodes.write(get_machine_desc(m) + '\n')
    f_nodes.close()
    return


def store_flows(flows, out_prefix):
    outpath = out_prefix + '.edges.txt'
    print('storing edges -> ' + outpath)
    f_flows = open(outpath, 'w')
    for f in flows:
        f_flows.write(get_flow_desc(f) + '\n')
    f_flows.close()
    return


def store_flow_graph(machines, flows, outpath):
    store_nodes(machines, outpath + '.nodes.txt')
    store_flows(flows, outpath + '.edges.txt')
    return


def gen_role_based_graph():
    client_roles = ['Dev', 'Admin', 'Mgmt', 'IT', 'HR']
    client_dist = [0.4, 0.15, 0.15, 0.15, 0.15]
    server_roles = ['SkypeEmail', 'CloudServices', 'Computing', 'Infra', 'Ext']
    server_dist = [0.3, 0.2, 0.1, 0.3, 0.1]
    # Skype Cloud Comp  Infr  Ext
    role_affinity = {'Dev': [0.20, 0.05, 0.50, 0.05, 0.20],  # Developers
                     'Admin': [0.40, 0.20, 0.05, 0.05, 0.30],  # Administrative
                     'Mgmt': [0.50, 0.20, 0.05, 0.05, 0.20],  # Management
                     'IT': [0.30, 0.30, 0.05, 0.30, 0.05],  # IT
                     'HR': [0.30, 0.30, 0.05, 0.05, 0.30]  # HR
                     }

    num_clients = 800
    num_servers = 200
    clients = get_machines(client_roles, client_dist, num_clients)
    servers = get_machines(server_roles, server_dist, num_servers)

    machines = add_node_ids(clients + servers)
    machine_map = get_machine_map(machines)

    # Directory where you want the edges and nodes files
    out_prefix = sys.argv[1]
    out_prefix += 'graph'
    store_nodes(machines, out_prefix)
    min_flows = 400
    max_flows = 500

    num_graphs = int(sys.argv[2])
    for graph_id in range(num_graphs):
        flows = []
        for c in clients:
            num_flows = np.random.random_integers(min_flows, max_flows)
            flow_id = 0
            proto_list = np.random.random_integers(1, 5, num_flows)
            data_vol_list = np.random.random_integers(1000, 1000000, num_flows)

            server_counts_by_role = Counter()
            servers_by_role = np.random.choice(server_roles, num_flows,
                                               p=role_affinity[c['type']])
            for server_type in servers_by_role:
                server_counts_by_role[server_type] += 1

            for s_t, counts in server_counts_by_role.items():
                dst_list = np.random.choice(machine_map[s_t], counts)
                for d in dst_list:
                    flows.append(make_flow(str(graph_id), c['id'], d['id'],
                                           proto_list[flow_id], data_vol_list[flow_id]))
                    flow_id += 1
        store_flows(flows, out_prefix + '_' + str(graph_id))
    return


def gen_random_flow_graph():
    num_clients = 800
    clients = get_machines(['Client-Mob', 'Client-PC'],
                           [0.2, 0.8], num_clients)
    num_servers = 200
    servers = get_machines(['Server-IT', 'Server-Biz', 'Server-Dev'],
                           [0.3, 0.4, 0.3], num_servers)
    machines = add_node_ids(clients + servers)
    out_prefix = 'data/random_graph'
    store_nodes(machines, out_prefix)
    min_flows = 100
    max_flows = 1000

    num_graphs = 10
    for graph_id in range(num_graphs):
        flows = []
        for c in clients:
            num_flows = np.random.random_integers(min_flows, max_flows)
            dst_list = [servers[d] for d in np.random.random_integers(0, num_servers - 1, num_flows)]
            proto_list = np.random.random_integers(1, 5, num_flows)
            data_vol_list = np.random.random_integers(1000, 1000000, num_flows)
            for i in range(len(dst_list)):
                flows.append(make_flow(str(graph_id), c['id'], dst_list[i]['id'], proto_list[i],
                                       data_vol_list[i]))
        store_flows(flows, out_prefix + '_' + str(graph_id))

    return


def get_machine_desc(m):
    info_list = [m['id'], m['type'], m['imp'], m['vul']]
    return '\t'.join(info_list)


def get_flow_desc(f):
    info_list = [f['timestamp'], f['src'], f['dst'], str(f['proto']), str(f['data_vol'])]
    return '\t'.join(info_list)


gen_role_based_graph()
