import abc
from collections import defaultdict, MutableMapping, OrderedDict
import json
import os
import sys
import random
import itertools
import math
import traceback
import warnings
import operator
import time
import numpy as np
import resource
import platform
from sklearn.metrics import average_precision_score


__author__ = 'Kelvin Gu'


def time_it(f, n=1):
    """
    My version of timeit
    """
    start = time.time()
    for k in range(n):
        f()
    stop = time.time()
    return stop - start


def npa(l):
    return np.array(l, dtype=float)


def npnorm(*args, **kwargs):
    """
    Just an abbreviation
    """
    return np.random.normal(*args, **kwargs)


def unit_vec(dim, entry):
    """
    Return column vector (2D numpy array) with all zeros except a 1 in the specified entry
    """
    x = np.zeros((dim, 1))
    x[entry] = 1
    return x


def print_nn(s):
    # print with no newline
    sys.stdout.write(s)
    sys.stdout.flush()


def sample_if_large(arr, max_size, replace=True):
    if len(arr) > max_size:
        idx = np.random.choice(len(arr), size=max_size, replace=replace)
        return [arr[i] for i in idx]

    return list(arr)


def sample_excluding(items, exclude):
    candidates = list(items)  # shallow copy
    random.shuffle(candidates)
    for cand in candidates:
        if cand not in exclude:
            return cand
    return None


def flatten(lol):
    """
    Flatten a list of lists
    """
    return [item for sublist in lol for item in sublist]


def chunks(l, n):
    """
    Return a generator of lists, each of size n (the last list may be less than n)
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


# PROFILING CODE
# ----- ----- ----- ----- ----- ----- ----- ----- ----- -----
profiling = False
profile_by_count = True  # TODO: WARNING, by_count breaks PyCharm debugger


def get_profiler():
    if not profiling:
        return

    # lazy load (this won't be available in production)
    import line_profiler

    glob = globals()

    if 'line_profiler_' not in glob:
        profiler = line_profiler.LineProfiler()
        if profile_by_count:
            profiler.enable_by_count()
        glob['line_profiler_'] = profiler
        print 'initialized profiler'

    return glob['line_profiler_']


def reset_profiler(keep_fxns=True):
    if not profiling:
        return

    # save old functions
    profiler = get_profiler()
    old_fxns = list(profiler.functions)

    # reset
    del globals()['line_profiler_']
    profiler = get_profiler()

    # put old functions back
    if keep_fxns:
        for fxn in old_fxns:
            profiler.add_function(fxn)
        print 'kept functions:', profiler.functions


def profile(f):
    """A decorator for functions you want to profile"""
    if not profiling:
        return f

    profiler = get_profiler()
    profiler.add_function(f)
    print 'added to profiler:', f
    return f


def profile_report():
    if not profiling:
        return
    profiler = get_profiler()
    profiler.print_stats()

# ----- ----- ----- ----- ----- ----- ----- ----- ----- -----


def memoize(f):
    cache = {}

    def decorated(*args):
        if args not in cache:
            cache[args] = f(*args)
        else:
            print 'loading cached values for {}'.format(args)
        return cache[args]

    return decorated


class EqualityMixin(object):
    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


def data_split(items, dev_part=0.1, test_part=0.1):
    # don't allow duplicates
    assert len(set(items)) == len(items)

    # remaining portion is set aside for train
    assert dev_part + test_part < 1.0

    items_copy = list(items)
    random.shuffle(items_copy)

    n = len(items_copy)
    ndev = int(n * dev_part)
    ntest = int(n * test_part)

    dev = items_copy[:ndev]
    test = items_copy[ndev:ndev + ntest]
    train = items_copy[ndev + ntest:]

    # verify that there is no overlap
    train_set = set(train)
    dev_set = set(dev)
    test_set = set(test)

    assert len(train_set.intersection(dev_set)) == 0
    assert len(train_set.intersection(test_set)) == 0

    print 'train {}, dev {}, test {}'.format(len(train), len(dev), len(test))
    return train, dev, test


def is_vector(x):
    # suppose x.shape = (d1, d2, ..., dn)
    # checks that there is no more than one di > 1
    non_flat = [i for i, d in enumerate(x.shape) if d > 1]
    return len(non_flat) <= 1


class MultivariateFunction(object):
    """
    Represents an Rm -> Rn nonlinearity
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def value(self, x):
        """
        m -> n
        """
        return

    def elem_derivative(self, x):
        """
        Element-wise derivative. m -> n
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def jacobian(self, x):
        """
        Jacobian. m -> n x m
        """
        assert is_vector(x)
        x_1d = np.ravel(x)

        # if an element-wise derivative is implemented, use that
        elem_deriv = self.elem_derivative(x_1d)
        return np.diag(elem_deriv)


class Tanh(MultivariateFunction):

    def value(self, x):
        return np.tanh(x)

    def elem_derivative(self, x):
        return 1.0 - np.tanh(x) ** 2

    def jacobian(self, x):
        return super(Tanh, self).jacobian(x)


class Identity(MultivariateFunction):
    def value(self, x):
        return x

    def elem_derivative(self, x):
        return np.ones(x.shape)

    def jacobian(self, x):
        return super(Identity, self).jacobian(x)


class Sigmoid(MultivariateFunction):
    def value(self, x):
        # scipy.special.expit will return NaN if x gets larger than about 700, which is just wrong

        # compute using two different approaches
        # they are each stable over a different interval of x
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')

            numer = np.exp(x)
            s0 = numer / (1.0 + numer)

            denom = 1.0 + np.exp(-x)
            s1 = 1.0 / denom

        # replace nans
        if isinstance(x, float):
            if np.isnan(s0):
                s0 = s1
        else:
            nans = np.isnan(s0)
            s0[nans] = s1[nans]

        return s0

    def elem_derivative(self, x):
        ex = np.exp(x)
        s = 1.0 / (1.0 + ex)
        return s - s ** 2

    def jacobian(self, x):
        return super(Sigmoid, self).jacobian(x)


# shortcuts
sigmoid_object = Sigmoid()
sigmoid = sigmoid_object.value
sigmoid_derivative = sigmoid_object.elem_derivative


class RectLinear(MultivariateFunction):
    def value(self, x):
        return np.maximum(x, 0.0)

    def elem_derivative(self, x):
        grad = np.zeros(x.shape)
        grad[x > 0.0] = 1.0
        return grad

    def jacobian(self, x):
        return super(RectLinear, self).jacobian(x)


def examine_nan(type, flag):
    # this gets called whenever numpy produces a NaN
    pass  # we can set a breakpoint here


def catch_nans():
    np.seterr(invalid='call')
    np.seterrcall(examine_nan)


def pandas_options():
    # lazy load
    import pandas as pd
    pd.set_option('expand_frame_repr', False)
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_colwidth', 1000)


def group(items, keyfunc):
    # this is different from itertools.groupby in several ways:
    # - groupby creates a new group every time the key CHANGES in the sequence of items
    # - this function returns a dict

    groups = defaultdict(list)
    for item in verboserate(items):
        l = groups[keyfunc(item)]
        l.append(item)
    return groups


def weighted_abs_error(examples, predictions):
    total = 0.0
    error = 0.0
    for ex, predict in zip(examples, predictions):
        w = ex.weight
        actual = ex.score
        error += w * abs(actual - predict)
        total += w

    return error / total


def f1(correct, retrieved):
    hits = float(len([a for a in retrieved if a in correct]))

    precision = hits / len(retrieved)
    recall = hits / len(correct)

    if precision + recall == 0:
        return 0.0

    return 2.0 * (precision * recall) / (precision + recall)


def dialogue_accuracy(dlg, predictions, exclude=None):
    if exclude is None:
        exclude = set()

    right = 0.0
    total = 0.0
    for correct, predict in itertools.izip(dlg.outputs, predictions):
        if correct in exclude:
            continue

        right += 1.0 if correct == predict else 0.0
        total += 1.0

    return right / total


def compute_if_absent(d, key, keyfunc):
    val = d.get(key)
    if val is None:
        val = keyfunc(key)
        d[key] = val
    return val


def tensor_combine(tensor, matrix):
    # linearly combine pages of the tensor, based on matrix columns
    # e.g. result[0] = tensor[0] * matrix[0, 0] + tensor[1] * matrix[1, 0] + ...
    # e.g. result[1] = tensor[0] * matrix[0, 1] + tensor[1] * matrix[1, 1] + ...

    # temporarily make the first axis the last
    axes = range(len(tensor.shape))
    tensor_mod = np.transpose(tensor, np.roll(axes, -1))

    # perform linear combination
    tensor_mod = tensor_mod.dot(matrix)

    # go back to original axes
    tensor_mod = np.transpose(tensor_mod, np.roll(axes, 1))

    return tensor_mod


class NestedDict(MutableMapping):
    def __init__(self):
        self.d = {}

    def __iter__(self):
        return self.d.__iter__()

    def __delitem__(self, key):
        return self.d.__delitem__(key)

    def __getitem__(self, key):
        try:
            return self.d.__getitem__(key)
        except KeyError:
            val = NestedDict()
            self.d[key] = val
            return val

    def __len__(self):
        return self.d.__len__()

    def __setitem__(self, key, value):
        return self.d.__setitem__(key, value)

    def get_nested(self, keys):
        d = self
        for k in keys:
            d = d[k]
        return d

    def set_nested(self, keys, val):
        d = self.get_nested(keys[:-1])
        return d.__setitem__(keys[-1], val)

    def __repr__(self):
        return self.d.__repr__()

    def as_dict(self):
        items = []
        for key, sub in self.iteritems():
            if isinstance(sub, NestedDict):
                val = sub.as_dict()
            else:
                val = sub
            items.append((key, val))
        return dict(items)


meta = NestedDict()


def metadata(keys, val):
    """
    Sets entries in a nested dictionary called meta.
    After each call, meta is updated and saved to meta.json in the current directory

    keys = either a string or a tuple of strings
    a tuple of strings will be interpreted as nested keys in a dictionary, i.e. dictionary[key1][key2][...]
    """
    # This is only designed to be used with CodaLab
    if isinstance(keys, tuple):
        meta.set_nested(keys, val)
    else:
        # if there is actually just one key
        meta[keys] = val

    # sync with file
    with open('meta.json', 'w') as f:
        d = meta.as_dict()  # json only handles dicts
        json.dump(d, f)


class ComputeDefaultDict(MutableMapping):
    def __init__(self, init_fxn):
        self.d = {}
        self.init_fxn = init_fxn

    def __iter__(self):
        return self.d.__iter__()

    def __delitem__(self, key):
        return self.d.__delitem__(key)

    def __getitem__(self, key):
        try:
            return self.d.__getitem__(key)
        except KeyError:
            val = self.init_fxn(key)
            self.d[key] = val
            return val

    def __len__(self):
        return self.d.__len__()

    def __setitem__(self, key, value):
        return self.d.__setitem__(key, value)


class FallbackDict(MutableMapping):
    """
    Getting: try to get item from main dict. If failed, get from fallback dict.
    Setting: set items in the main dict. If you try to set an item present in the fallback dict, throw an error.
    """

    def __init__(self, main, fallback):

        # assert no key overlap
        # TODO: WARNING, may be expensive
        # main_keys = set(main.keys())
        # fback_keys = set(fallback.keys())
        # assert len(main_keys.intersection(fback_keys)) == 0

        self.main = main
        self.fallback = fallback

    def __getitem__(self, key):
        try:
            return self.main[key]
        except KeyError:
            return self.fallback[key]

    def __setitem__(self, key, value):
        if key in self.fallback:
            raise KeyError('Not allowed to set items in fallback dict')
        self.main[key] = value

    def __delitem__(self, key):
        if key in self.fallback:
            raise KeyError('Not allowed to delete items in fallback dict')
        del self.main[key]

    def __iter__(self):
        return itertools.chain(iter(self.main), iter(self.fallback))

    def __len__(self):
        return len(self.main) + len(self.fallback)

    def __repr__(self):
        return 'main:\n{}\nfallback:\n{}'.format(repr(self.main), repr(self.fallback))


def nearest_word(v, wvecs):
    word_scores = []
    for word, vec in wvecs.iteritems():
        s = vec.T.dot(v)[0][0]
        word_scores.append((word, s))

    top_word, top_score = max(word_scores, key=operator.itemgetter(1))
    return top_word


def align_view(words, width=7):
    return ' '.join([word.ljust(width) for word in words])


def format_nested_dict(d):
    # convert all keys and leaf values to strings
    def string_keys(d0):
        if not isinstance(d0, dict):
            return str(d0)
        return dict((str(k), string_keys(v)) for k, v in d0.iteritems())

    return json.dumps(string_keys(d), sort_keys=True, indent=4)


def nested_iteritems(d):
    for k, v in d.iteritems():
        if isinstance(v, dict):
            for k_suffix, v_leaf in nested_iteritems(v):
                yield (k,) + k_suffix, v_leaf
        else:
            yield (k,), v


def nested_setitem(d, key_tuple, val):
    sub_d = d
    for key in key_tuple[:-1]:
        sub_d = compute_if_absent(sub_d, key, lambda k: {})

    sub_d[key_tuple[-1]] = val


def transform_nested(d0, fxn):
    nested = lambda: defaultdict(nested())
    d = {}

    for key_tuple, val in nested_iteritems(d0):
        new_key_tuple, new_val = fxn(key_tuple, val)
        nested_setitem(d, new_key_tuple, new_val)

    return d


def unit_circle_points(n):
    """
    Return n unique evenly spaced points on the unit circle
    """
    thetas = np.linspace(0, 2 * math.pi, num=n, endpoint=False)
    radii = np.ones(thetas.shape)

    x = radii * np.cos(thetas)
    y = radii * np.sin(thetas)

    xy = np.vstack((x, y))

    pts = [xy[:, [i]] for i in range(len(thetas))]

    return pts


def conveyor_belt(n, shift):
    w = np.eye(n)
    cycle = [(i - shift) % n for i in range(n)]
    w = w[cycle, :]
    w[:shift, :] = 0.0
    return w


def verboserate(iterable, time_wait=5, report=None):
    """
    Iterate verbosely.
    """
    try:
        total = len(iterable)
    except TypeError:
        total = '?'

    def default_report(steps, elapsed):
        print '{} of {} processed ({} s)'.format(steps, total, elapsed)
        sys.stdout.flush()

    if report is None:
        report = default_report

    start = time.time()
    prev = start
    for steps, val in enumerate(iterable):
        current = time.time()
        since_prev = current - prev
        elapsed = current - start
        if since_prev > time_wait:
            report(steps, elapsed)
            prev = current
        yield val


def in_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False


def makedirs(directory):
    if directory != '' and not os.path.exists(directory):
        os.makedirs(directory)


def show(title, directory=''):
    import matplotlib.pyplot as plt
    if in_ipython():
        plt.show()
    else:
        # ensure directory exists
        makedirs(directory)

        plt.savefig(os.path.join(directory, title) + '.png')
        # close all figures to conserve memory
        plt.close('all')


def ticks_off():
    tickparams = dict((key, 'off') for key in ['top', 'bottom', 'left', 'right', 'labelbottom', 'labelleft'])
    tickparams['which'] = 'both'
    import matplotlib.pyplot as plt
    plt.tick_params(**tickparams)


def matshow(mat):
    import matplotlib.pyplot as plt
    plt.figure()
    plt.imshow(mat, interpolation='nearest')
    show('matshow')


class Bunch:
    """
    A simple class for holding arbitrary attributes.
    """
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

    def __repr__(self):
        return str(self.__dict__.keys())


def pad_vector(vec, before, after):
    return np.pad(vec, ((before, after), (0, 0)), mode='constant', constant_values=0)


def latex_subscript(main, *subscript_terms):
    return '{}_{{{}}}'.format(main, ','.join([str(term) for term in subscript_terms]))


def invert_latex_subscript(s):
    main, subscript = s.split('_', 1)

    # strip leading and trailing braces
    subscript = subscript[1:-1]

    # split by commas
    split_pts = []
    brace_depth = 0
    for i, char in enumerate(subscript):
        if char == '{': brace_depth += 1
        if char == '}': brace_depth -= 1
        if brace_depth == 0 and char == ',':
            split_pts.append(i)

    split_pts.insert(0, -1)
    split_pts.append(len(subscript))

    subscript_terms = []
    for i in range(len(split_pts) - 1):
        term = subscript[split_pts[i]+1:split_pts[i+1]]
        subscript_terms.append(term)

    return main, subscript_terms


class Stopwatch(dict):
    def __init__(self):
        self.start = time.time()

    def mark(self, name):
        diff = time.time() - self.start
        self[name] = diff
        return diff


def avg_scaling(a, nonlinearity=None):
    """
    Estimate the expected scaling factor: ||f(Av)|| / ||v||
    Where f is a nonlinearity, v is a random unit vector and we use the 2-norm
    """
    if nonlinearity is None:
        nonlinearity = Identity()

    q = 5000
    V = np.random.normal(0.0, 1.0, (a.shape[1], q))
    ratios = []
    for k in range(q):
        v = V[:, k].reshape(-1, 1)
        y = nonlinearity.value(a.dot(v))
        ratio = np.linalg.norm(y) / np.linalg.norm(v)
        ratios.append(ratio)
    return sum(ratios) / len(ratios)


def average_precision(positives, negatives):
    """
    positives and negatives must each be a 1D array of scores
    """
    if len(positives) == 0:
        print 'WARNING: No positive examples presented! AP = NaN.'
        return np.nan
    elif len(negatives) == 0:
        print 'WARNING: No negative examples presented! AP = 1.'
        return 1.
    scores = np.concatenate((positives, negatives))
    labels = np.concatenate((np.ones(positives.shape), np.zeros(negatives.shape)))
    return average_precision_score(labels, scores)


def ranks(scores, ascending=True):
    if isinstance(scores, list):
        scores = np.array(scores)
    else:
        assert len(scores.shape) == 1

    flip = 1 if ascending else -1
    idx = np.argsort(flip * scores)
    ranks = np.empty(scores.shape, dtype=int)
    ranks[idx] = np.arange(len(scores))
    # ranks should start from 1
    ranks += 1
    return list(ranks)


def quantile(rank, total):
    """
    Return 1.0 when you are first, 0.0 when you are last.
    """
    if total == 1:
        return np.nan
    return float(total - rank) / (total - 1)


def rank_from_quantile(quantile, total):
    if np.isnan(quantile):
        return 1
    return total - quantile * (total - 1)


def average_quantile(positives, negatives):
    all = np.concatenate((positives, negatives))
    all_ranks = ranks(all, ascending=False)[:len(positives)]
    pos_ranks = ranks(positives, ascending=False)
    filtered_ranks = [a - (p - 1) for a, p in itertools.izip(all_ranks, pos_ranks)]  # filtered ranks
    n = len(negatives) + 1  # total filtered candidates
    quantiles = [quantile(r, n) for r in filtered_ranks]
    return np.nanmean(quantiles)


def plot_ecdf(x, *args, **kwargs):
    x = list(x)  # make a copy
    x.sort()
    y = (np.arange(len(x)) + 1.0) / len(x)
    import matplotlib.pyplot as plt
    plt.plot(x, y, *args, **kwargs)


def plot_pdf(x, cov_factor=None, *args, **kwargs):
    import matplotlib.pyplot as plt
    from scipy.stats import gaussian_kde
    density = gaussian_kde(x)
    xgrid = np.linspace(min(x), max(x), 200)
    if cov_factor is not None:
        density.covariance_factor = lambda: cov_factor
        density._compute_covariance()
    y = density(xgrid)
    plt.plot(xgrid, y, *args, **kwargs)


def sorted_by_value(d, ascending=True):
    return OrderedDict(sorted(d.items(), key=operator.itemgetter(1), reverse=not ascending))


def show_warn_traceback():
    def warn_with_traceback(message, category, filename, lineno, file=None, line=None):
        traceback.print_stack()
        log = file if hasattr(file, 'write') else sys.stderr
        log.write(warnings.formatwarning(message, category, filename, lineno, line))
    warnings.showwarning = warn_with_traceback


def gb_used():
    used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if platform.system() != 'Darwin':
        # on Linux, used is in terms of kilobytes
        power = 2
    else:
        # on Mac, used is in terms of bytes
        power = 3
    return float(used) / math.pow(1024, power)


def transpose_dict(d):
    d_t = defaultdict(dict)
    for i, di in d.iteritems():
        for j, dij in di.iteritems():
            d_t[j][i] = dij
    return dict(d_t)


def best_threshold(scores, labels, debug=False):
    # find best threshold in O(nlogn)
    # does not handle scores of infinity or -infinity
    items = zip(scores, labels)
    items.sort()
    total = len(items)
    total_pos = len([l for l in labels if l])

    def accuracy(p, n):
        correct_n = n
        correct_p = total_pos - p
        return float(correct_n + correct_p) / total

    # predict True iff score > thresh
    pos = 0  # no. pos <= thresh
    neg = 0  # no. neg <= thresh

    thresh_accs = [(float('-inf'), accuracy(pos, neg))]
    for thresh, label in items:
        if label:
            pos += 1
        else:
            neg += 1
        thresh_accs.append((thresh, accuracy(pos, neg)))

    if debug:
        import matplotlib.pyplot as plt
        x, y = zip(*thresh_accs)
        plt.figure()
        plt.plot(x, y)
        pos_scores = [s for s, l in items if l]
        neg_scores = [s for s, l in items if not l]
        plot_pdf(pos_scores, 0.1, color='b')
        plot_pdf(neg_scores, 0.1, color='r')
        plt.show()

    return max(thresh_accs, key=operator.itemgetter(1))[0]
