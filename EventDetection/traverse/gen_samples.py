import numpy as np
import sys
import util
from collections import defaultdict, Counter
import random
import itertools
import fast_graph_traversal as graph_traversal
from random import shuffle
from copy import deepcopy

# Construct graph from triples:

# Positive samples:
# Choose a node randomly
# Choose a relation for that node randomly
# Choose a node (if there are multiple nodes) at the end of this relation
# Do this for path_len number of times
# Do the above for num_nodes_to_sample

# Negative samples:
# Gather a list of entities that can't be used: neighbors of neighbors
# Pick a random entity in the graph until it is not in the above list
# Substitute the current entity with this new one


# defines whether an edge is inverted or not

class PathQuery(object):
    def __init__(self, s, r, t):
        # at least one of s or t must be a string
        assert isinstance(s, str) or isinstance(t, str)
        assert isinstance(r, tuple)  # tuple rather than list. Tuples are hashable.
        self.s = s  # src word(s)
        self.r = r  # relation word(s)
        self.t = t  # target word(s)

    def __repr__(self):
        rep = '{} {} {}'.format(self.s, self.r, self.t)
        if hasattr(self, 'label'):
            rep += ' {}'.format(self.label)
        return rep

    def __eq__(self, other):
        if not isinstance(other, PathQuery):
            return False
        return self.s == other.s and self.r == other.r and self.t == other.t

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self.s.__hash__() + self.r.__hash__() + self.t.__hash__()

    @property
    def r_inverted(self):
        return tuple(invert(rel) for rel in self.r[::-1])

    @staticmethod
    def from_triples(triples):
        return [PathQuery(s, (r,), t) for (s, r, t) in triples]

    @staticmethod
    def flatten(rwqs):
        # flatten batch queries into single queries
        flat = []
        for rwq in rwqs:
            for t, rwp in itertools.izip(rwq.t, rwq.label):
                rwq_new = PathQuery(rwq.s, rwq.r, t)
                rwq_new.label = rwp
                flat.append(rwq_new)
        return flat

    @staticmethod
    def batch(rwqs):
        # group random walk queries having the same source and relation sequence
        grouped = defaultdict(list)
        for rwq in rwqs:
            grouped[(rwq.s, rwq.r)].append(rwq)
        batched = []
        for (s, r), group in grouped.iteritems():
            t = [rwq.t for rwq in group]
            label = [rwq.label for rwq in group]
            rwq_new = PathQuery(s, r, t)
            rwq_new.label = label
            batched.append(rwq_new)
        return batched

    @staticmethod
    def stratify(rwqs):
        # stratified sampling (leaves input untouched)
        num_bins = 100
        samples_per_bin = len(rwqs) / num_bins

        bins = defaultdict(list)
        for rwq in rwqs:
            # NOTE: probability of 1.0 gets its own bin
            bin_idx = int(rwq.label * num_bins)
            bins[bin_idx].append(rwq)

        stratified = []
        for b in bins.values():
            sample = list(np.random.choice(b, samples_per_bin, replace=True))
            stratified.extend(sample)
        return stratified

    @staticmethod
    def stats(pqs):
        ents = Counter()
        rels = Counter()
        paths = Counter()
        lengths = Counter()
        for pq in util.verboserate(pqs):
            ents[pq.s] += 1
            ents[pq.t] += 1
            path = pq.r
            paths[path] += 1
            lengths[len(path)] += 1
            for r in path:
                rels[r] += 1
        return ents, rels, paths, lengths


class Graph(object):
    def __init__(self, triples):
        self.triples = triples
        neighbors = defaultdict(lambda: defaultdict(set))
        relation_args = defaultdict(lambda: defaultdict(set))

        print 'compiling graph...'
        for s, r, t in triples:
            relation_args[r]['s'].add(s)
            relation_args[r]['t'].add(t)
            neighbors[s][r].add(t)
            neighbors[t][invert(r)].add(s)

        def freeze(d):
            frozen = {}
            for key, subdict in d.iteritems():
                frozen[key] = {}
                for subkey, set_val in subdict.iteritems():
                    frozen[key][subkey] = tuple(set_val)
            return frozen

        # WARNING: both neighbors and relation_args must not have default initialization.
        # Default init is dangerous, because we sometimes perform uniform sampling over
        # all keys in the dictionary. This distribution will get altered if a user asks about
        # entities or relations that weren't present.

        # self.neighbors[start][relation] = (end1, end2, ...)
        # self.relation_args[relation][position] = (ent1, ent2, ...)
        # position is either 's' (domain) or 't' (range)
        self.neighbors = freeze(neighbors)
        self.relation_args = freeze(relation_args)
        self.random_entities = []

        cpp_graph = graph_traversal.Graph()
        for s, r, t in triples:
            cpp_graph.add_edge(s, r, t)
            cpp_graph.add_edge(t, invert(r), s)
        self.cpp_graph = cpp_graph

    def random_walk_probs(self, start, path):
        return self.cpp_graph.exact_random_walk_probs(start, list(path))

    @util.profile
    def walk_all(self, start, path, positive_branch_factor=float('inf')):
        if positive_branch_factor == 0:
            return set()

        approx = positive_branch_factor != float('inf')

        if approx:
            return set(self.cpp_graph.approx_path_traversal(start, list(path), positive_branch_factor))
        else:
            return set(self.cpp_graph.path_traversal(start, list(path)))

    def is_trivial_query(self, start, path):
        return self.cpp_graph.is_trivial_query(start, list(path))

    def type_matching_entities(self, path, position):
        if position == 's':
            r = path[0]
        elif position == 't':
            r = path[-1]
        else:
            raise ValueError(position)

        try:
            if not inverted(r):
                return self.relation_args[r][position]
            else:
                inv_pos = 's' if position == 't' else 't'
                return self.relation_args[invert(r)][inv_pos]
        except KeyError:
            # nothing type-matches
            return tuple()

    def random_walk(self, start, length, no_return=False):
        """
        If no_return, the random walk never revisits the same node. Can sometimes return None, None.
        """
        max_attempts = 1000
        for i in range(max_attempts):

            sampled_path = []
            visited = set()
            current = start
            for k in range(length):
                visited.add(current)

                r = random.choice(self.neighbors[current].keys())
                sampled_path.append(r)

                candidates = self.neighbors[current][r]

                if len(candidates) == 1:
                    sampled_path.append(candidates[0])
                elif len(candidates) > 1:
                    sampled_path.append(random.choice(candidates))
                else:
                    sampled_path.append('')

                if no_return:
                    current = util.sample_excluding(candidates, visited)
                else:
                    current = random.choice(candidates)

                # no viable next step
                if current is None:
                    break

            # failed to find a viable walk. Try again.
            if current is None:
                continue

            # return current
            return sampled_path

        return None, None

    def random_walk_no_loop(self, start, length, no_return=False):
        """
        If no_return, the random walk never revisits the same node. Can sometimes return None, None.
        """
        max_attempts = 1000
        for i in range(max_attempts):

            sampled_path = [start]
            visited = set()
            current = start
            for k in range(length):
                visited.add(current)

                r = random.choice(self.neighbors[current].keys())
                sampled_path.append(r)

                candidates = self.neighbors[current][r]

                if len(candidates) == 1:
                    cand = candidates[0]
                    if cand not in sampled_path:
                        sampled_path.append(cand)
                    # Well, this entity is already in the path. Giving up on path...
                    else:
                        sampled_path = []
                        break
                elif len(candidates) > 1:
                    cand = random.choice(candidates)
                    tries = 0
                    # Try 15 times to find an entity not in the path already
                    while cand in sampled_path and tries < 15:
                        cand = random.choice(candidates)
                        tries += 1
                    # Couldn't find such an entity, giving up on path
                    if cand in sampled_path:
                        sampled_path = []
                        break
                    sampled_path.append(cand)
                else:
                    sampled_path.append('')

                if no_return:
                    current = util.sample_excluding(candidates, visited)
                else:
                    current = random.choice(candidates)

                # no viable next step
                if current is None:
                    break

            # failed to find a viable walk. Try again.
            if current is None:
                continue

            # return current
            return sampled_path

        return None, None

    @staticmethod
    def sep_ntt_rel(path):
        ntt = []
        rels = []
        for i in range(0, len(path), 2):
            ntt.append(path[i])
        for i in range(1, len(path), 2):
            rels.append(path[i])
        return ntt, rels

    def random_walk_subs(self, path, no_of_subs):
        es, _ = self.sep_ntt_rel(path)
        no_of_e = len(es)
        tot_subs = min(no_of_e, no_of_subs)
        # Indices of entities to be subbed:
        idc = random.sample(xrange(0, no_of_e), tot_subs)
        first_level_neighbors = set()
        second_level_neighbors = set()
        for i in idc:
            for j in self.neighbors[es[i]]:
                first_level_neighbors.add(self.neighbors[es[i]][j])
        first_level_neighbors = [item for sublist in first_level_neighbors for item in sublist]
        for neigh in first_level_neighbors:
            # print neigh
            for j in self.neighbors[neigh]:
                second_level_neighbors.add(self.neighbors[neigh][j])
        second_level_neighbors = [item for sublist in second_level_neighbors for item in sublist]
        # Sub the entities
        es_to_sub = [es[i] for i in idc]
        for e in es_to_sub:
            re = self.random_entity()
            tries = 0
            neighborhood = first_level_neighbors + second_level_neighbors
            while re in neighborhood and tries < 100:
                re = self.random_entity()
                tries += 1
            if re in neighborhood:
                return []
            path[path.index(e)] = re
        return path

    @util.profile
    def random_walk_constrained(self, start, path):
        """
        Warning! Can sometimes return None.
        """

        # if start node isn't present we can't take this walk
        if start not in self.neighbors:
            return None

        current = start
        for r in path:
            rels = self.neighbors[current]
            if r not in rels:
                # no viable next steps
                return None
            current = random.choice(rels[r])
        return current

    def random_entity(self):
        if len(self.random_entities) == 0:
            self.random_entities = list(np.random.choice(self.neighbors.keys(), size=20000, replace=True))
        return self.random_entities.pop()

    @util.profile
    def random_path_query(self, length):

        while True:
            # choose initial entity uniformly at random
            source = self.random_entity()

            # sample a random walk
            path, target = self.random_walk(source, length)

            # Failed to find random walk. Try again.
            if path is None:
                continue

            pq = PathQuery(source, path, target)
            return pq

    def relation_stats(self):
        stats = defaultdict(dict)
        rel_counts = Counter(r for s, r, t in self.triples)

        for r, args in self.relation_args.iteritems():
            out_degrees, in_degrees = [], []
            for s in args['s']:
                out_degrees.append(len(self.neighbors[s][r]))
            for t in args['t']:
                in_degrees.append(len(self.neighbors[t][invert(r)]))

            domain = float(len(args['s']))
            range1 = float(len(args['t']))
            out_degree = np.mean(out_degrees)
            in_degree = np.mean(in_degrees)
            stat = {'avg_out_degree': out_degree,
                    'avg_in_degree': in_degree,
                    'min_degree': min(in_degree, out_degree),
                    'in/out': in_degree / out_degree,
                    'domain': domain,
                    'range': range1,
                    'r/d': range1 / domain,
                    'total': rel_counts[r],
                    'log(total)': np.log(rel_counts[r])
                    }

            # include inverted relation
            inv_stat = {'avg_out_degree': in_degree,
                        'avg_in_degree': out_degree,
                        'min_degree': stat['min_degree'],
                        'in/out': out_degree / in_degree,
                        'domain': range1,
                        'range': domain,
                        'r/d': domain / range1,
                        'total': stat['total'],
                        'log(total)': stat['log(total)']
                        }

            stats[r] = stat
            stats[invert(r)] = inv_stat

        return stats


def inverted(r):
    return r[:2] == '**'


def invert(r):
    if inverted(r):
        return r[2:]
    else:
        return '**' + r


def parse_dataset(data_path, maximum_examples=float('inf')):
    def get_examples(filename):
        # filename = join(data_path, pname)

        examples_arr = list()
        with open(filename, 'r') as f:
            num_examples = 0
            for line in util.verboserate(f):
                if float(num_examples) >= maximum_examples:
                        break
                items = line.split()
                s, path, t = items[:3]
                rels = tuple(path.split(','))
                entities.add(s)
                entities.add(t)
                relations.update(rels)

                if len(items) >= 4:
                    label = items[3]
                else:
                    label = '1'  # if no label, assume positive

                # only add positive examples
                if label == '1':
                    examples_arr.append(PathQuery(s, rels, t))
                    num_examples += 1

        return examples_arr

    def get_triples(queries):
        triples_arr = list()
        for query in queries:
            if len(query.r) == 1:
                triples_arr.append((query.s, str(query.r[0]), query.t))
        return triples_arr

    entities = set()
    relations = set()

    # add datasets
    attributes = dict()
    # 'full' is going to be train + test
    attributes['full'] = get_examples(data_path)
    attributes['entities'] = list(entities)
    attributes['relations'] = list(relations)

    # add graphs
    triples = dict()
    triples['full'] = get_triples(attributes['full'])
    attributes['full_graph'] = Graph(triples['full'])

    return attributes
    # return util.Bunch(**attributes)


def rw_test(dataset, wpn, length):
    graph = dataset['full_graph']
    entities = dataset['entities']
    paths = []
    # i = 0
    for _ in range(0, wpn):
        for e in entities:
            # if i > 100:
            #     break
            path = graph.random_walk_no_loop(e, length)
            # i += 1
            if path:
                paths.append(path)
    return paths


def rw_subs_test(dataset, paths, subs):
    graph = dataset['full_graph']
    neg_paths = []
    for path in paths:
        neg_path = graph.random_walk_subs(path, subs)
        if neg_path:
            neg_paths.append(neg_path)
    return neg_paths


# Convert the lists to strings
# Write the generated samples to file
def file_write(fname, samples):
    f = open(fname, 'wb')
    for sampl in samples:
        s = ''
        for word in sampl:
            s += word + ' '
        s = s.rstrip()
        f.write(s)
        f.write('\n')
    f.write('\n')
    f.close()


data_directory = sys.argv[1]
dset = parse_dataset(data_directory)

walks_per_ntt = 7
path_len = 7
pos_samples = rw_test(dset, walks_per_ntt, path_len)
temp_pos_samples = deepcopy(pos_samples)

# for ps in pos_samples:
#     print ps
#
# print '-----------------------------'
# print '-----------------------------'
# print '-----------------------------'

subs_in_samples = 5
neg_samples = rw_subs_test(dset, pos_samples, subs_in_samples)

# for ns in neg_samples:
#     print ns
#
# Add labels:
# Positive label = 0
for sample in temp_pos_samples:
    sample.append('0')

# Negative label = 1
for sample in neg_samples:
    sample.append('1')

all_samples = temp_pos_samples + neg_samples

# Shuffle the samples
shuffle(all_samples)
directory = 'data/wordnet/versions/'
file_write(directory + 'all_samples_' + str(subs_in_samples) + 'f_pl' + str(path_len) + '.txt', all_samples)
file_write(directory + 'pos_samples_' + str(subs_in_samples) + 'f_pl' + str(path_len) + '.txt', temp_pos_samples)
file_write(directory + 'neg_samples_' + str(subs_in_samples) + 'f_pl' + str(path_len) + '.txt', neg_samples)
