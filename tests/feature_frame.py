"""Small eager evaluator for the Spark expressions used by the feature parity fixture.

This is a local test double, not Spark integration validation. Both the frozen
pre-refactor oracle and production definitions execute against the same fixture.
"""
import math
import operator
import statistics
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

# Fixture epochs are in January 1970, when Rome uses UTC+1; no tzdata dependency.
ROME = timezone(timedelta(hours=1))


def expr(value):
    return value if isinstance(value, Expr) else Expr(lambda f, i: value, infer(value))


def col(value):
    return Expr(lambda f, i: f.rows[i][value], lambda f: f.types[value]) if isinstance(value, str) else expr(value)


def infer(value):
    if isinstance(value, bool): return 'boolean'
    if isinstance(value, int): return 'int'
    if isinstance(value, float): return 'double'
    if isinstance(value, datetime): return 'timestamp'
    if isinstance(value, list): return 'array<string>'
    if value is None: return 'void'
    return 'string'


class Expr:
    def __init__(self, fn, dtype='double'):
        self.fn, self.dtype = fn, dtype

    def typ(self, frame):
        return self.dtype(frame) if callable(self.dtype) else self.dtype

    def binary(self, other, op, dtype=None):
        other = expr(other)
        def evaluate(frame, i):
            a, b = self.fn(frame, i), other.fn(frame, i)
            return None if a is None or b is None else op(a, b)
        return Expr(evaluate, dtype or (lambda f: 'double' if 'double' in [self.typ(f), other.typ(f)] else self.typ(f)))

    def __add__(self, x): return self.binary(x, operator.add)
    def __sub__(self, x): return self.binary(x, operator.sub)
    def __rsub__(self, x): return expr(x).binary(self, operator.sub)
    def __mul__(self, x): return self.binary(x, operator.mul)
    def __rmul__(self, x): return expr(x).binary(self, operator.mul)
    def __truediv__(self, x): return self.binary(x, operator.truediv, 'double')
    def __gt__(self, x): return self.binary(x, operator.gt, 'boolean')
    def __ge__(self, x): return self.binary(x, operator.ge, 'boolean')
    def __lt__(self, x): return self.binary(x, operator.lt, 'boolean')
    def __le__(self, x): return self.binary(x, operator.le, 'boolean')
    def __eq__(self, x): return self.binary(x, operator.eq, 'boolean')
    def __ne__(self, x): return self.binary(x, operator.ne, 'boolean')
    def __and__(self, x): return self.binary(x, lambda a, b: bool(a and b), 'boolean')
    def __or__(self, x):
        x = expr(x)
        def evaluate(frame, index):
            a, b = self.fn(frame, index), x.fn(frame, index)
            return True if a is True or b is True else None if a is None or b is None else False
        return Expr(evaluate, 'boolean')
    def __invert__(self): return Expr(lambda f, i: None if self.fn(f, i) is None else not self.fn(f, i), 'boolean')
    def isNull(self): return Expr(lambda f, i: self.fn(f, i) is None, 'boolean')
    def isNotNull(self): return ~self.isNull()
    def eqNullSafe(self, other):
        other = expr(other)
        return Expr(lambda f, i: self.fn(f, i) == other.fn(f, i), 'boolean')
    def between(self, lo, hi): return (self >= lo) & (self <= hi)
    def isin(self, *values): return Expr(lambda f, i: self.fn(f, i) in values, 'boolean')
    def cast(self, dtype):
        convert = {'int': int, 'long': int, 'double': float, 'string': str, 'timestamp': lambda x: x}[dtype]
        return Expr(lambda f, i: None if self.fn(f, i) is None else convert(self.fn(f, i)),
                    'bigint' if dtype == 'long' else dtype)


class When(Expr):
    def __init__(self, condition, value):
        self.condition, self.value = expr(condition), expr(value)
        super().__init__(lambda f, i: self.value.fn(f, i) if self.condition.fn(f, i) else None, self.value.dtype)

    def otherwise(self, value):
        value = expr(value)
        return Expr(lambda f, i: self.value.fn(f, i) if self.condition.fn(f, i) else value.fn(f, i), self.value.dtype)


class WindowSpec:
    def __init__(self, keys=(), order=None, bounds=None, mode=None):
        self.keys, self.order, self.bounds, self.mode = keys, order, bounds, mode
    def orderBy(self, name): return WindowSpec(self.keys, name)
    def rowsBetween(self, lo, hi): return WindowSpec(self.keys, self.order, (lo, hi), 'rows')
    def rangeBetween(self, lo, hi): return WindowSpec(self.keys, self.order, (lo, hi), 'range')
    def peers(self, frame, index):
        row = frame.rows[index]
        peers = [i for i, r in enumerate(frame.rows) if all(r[k] == row[k] for k in self.keys)]
        return sorted(peers, key=lambda i: frame.rows[i][self.order])
    def members(self, frame, index):
        peers = self.peers(frame, index)
        if self.mode == 'range':
            v = frame.rows[index][self.order];lo, hi = self.bounds
            return [j for j in peers if v + lo <= frame.rows[j][self.order] <= v + hi]
        if self.mode == 'rows':
            position = peers.index(index);lo, hi = self.bounds
            return peers[max(0, position + lo):position + hi + 1]
        return peers


class Window:
    unboundedPreceding = float('-inf')
    partitionBy = staticmethod(lambda *keys: WindowSpec(keys))


class WindowExpr:
    def __init__(self, name, operation, offset=0):
        self.value, self.operation, self.offset = col(name), operation, offset
    def over(self, window):
        def calculate(frame, index):
            peers = window.peers(frame, index)
            if self.operation in ['lag', 'lead']:
                j = peers.index(index) + (self.offset if self.operation == 'lead' else -self.offset)
                return self.value.fn(frame, peers[j]) if 0 <= j < len(peers) else None
            values = [self.value.fn(frame, j) for j in window.members(frame, index)]
            values = [x for x in values if x is not None]
            if self.operation == 'count': return len(values)
            if not values: return None
            if self.operation == 'avg': return sum(values) / len(values)
            if self.operation == 'max': return max(values)
            if self.operation == 'std': return statistics.stdev(values) if len(values) > 1 else None
            raise AssertionError(self.operation)
        dtype = 'double' if self.operation in ['avg', 'std'] else 'bigint' if self.operation == 'count' else self.value.dtype
        return Expr(calculate, dtype)


class Functions:
    col = staticmethod(col)
    lit = staticmethod(expr)
    when = When
    lag = staticmethod(lambda name, offset=1: WindowExpr(name, 'lag', offset))
    lead = staticmethod(lambda name, offset=1: WindowExpr(name, 'lead', offset))
    avg = staticmethod(lambda name: WindowExpr(name, 'avg'))
    max = staticmethod(lambda name: WindowExpr(name, 'max'))
    count = staticmethod(lambda name: WindowExpr(name, 'count'))
    stddev_samp = staticmethod(lambda name: WindowExpr(name, 'std'))
    @staticmethod
    def unary(value, operation, dtype='double'):
        value = col(value)
        return Expr(lambda f, i: None if value.fn(f, i) is None else operation(value.fn(f, i)), dtype)
    abs = staticmethod(lambda x: Functions.unary(x, abs, col(x).dtype))
    radians = staticmethod(lambda x: Functions.unary(x, math.radians))
    sin = staticmethod(lambda x: Functions.unary(x, math.sin))
    cos = staticmethod(lambda x: Functions.unary(x, math.cos))
    asin = staticmethod(lambda x: Functions.unary(x, math.asin))
    sqrt = staticmethod(lambda x: Functions.unary(x, math.sqrt))
    pow = staticmethod(lambda x, y: col(x).binary(y, operator.pow, 'double'))
    size = staticmethod(lambda x: Functions.unary(x, len, 'int'))
    hour = staticmethod(lambda x: Functions.unary(x, lambda d: d.hour, 'int'))
    minute = staticmethod(lambda x: Functions.unary(x, lambda d: d.minute, 'int'))
    month = staticmethod(lambda x: Functions.unary(x, lambda d: d.month, 'int'))
    dayofmonth = staticmethod(lambda x: Functions.unary(x, lambda d: d.day, 'int'))
    dayofweek = staticmethod(lambda x: Functions.unary(x, lambda d: (d.weekday() + 1) % 7 + 1, 'int'))
    timestamp_seconds = staticmethod(lambda x: Functions.unary(x, lambda t: datetime.fromtimestamp(t, ROME), 'timestamp'))
    @staticmethod
    def coalesce(*values):
        values = [col(v) for v in values]
        return Expr(lambda f, i: next((v.fn(f, i) for v in values if v.fn(f, i) is not None), None), values[0].dtype)
    @staticmethod
    def least(*values):
        values = [col(v) for v in values]
        return Expr(lambda f, i: min(v.fn(f, i) for v in values), 'double')
    @staticmethod
    def greatest(*values):
        values = [col(v) for v in values]
        return Expr(lambda f, i: max(v.fn(f, i) for v in values), 'double')
    @staticmethod
    def expr(sql):
        name = sql.split('`')[1]
        return col(name).cast('double')


class Frame:
    def __init__(self, rows, types=None):
        self.rows = rows
        self.types = types or {k: infer(v) for k, v in rows[0].items()}
    @property
    def columns(self): return list(self.types)
    @property
    def schema(self):
        return SimpleNamespace(fields=[SimpleNamespace(name=k, dataType=SimpleNamespace(simpleString=lambda v=v: v))
                                       for k, v in self.types.items()])
    def withColumn(self, name, value):
        value = expr(value)
        types = dict(self.types);types[name] = value.typ(self)
        return Frame([dict(row, **{name: value.fn(self, i)}) for i, row in enumerate(self.rows)], types)
    def filter(self, condition): return Frame([r for i, r in enumerate(self.rows) if condition.fn(self, i)], self.types)
    def limit(self, n): return Frame(self.rows[:n], self.types)
    def count(self): return len(self.rows)
    def select(self, *names): return Frame([{k: r[k] for k in names} for r in self.rows], {k: self.types[k] for k in names})
    def groupBy(self, *keys):
        frame = self
        class Group:
            def count(self):
                counts = {}
                for r in frame.rows:
                    key = tuple(r[k] for k in keys);counts[key] = counts.get(key, 0) + 1
                return Frame([dict(zip(keys, key), count=n) for key, n in counts.items()],
                             dict({k: frame.types[k] for k in keys}, count='bigint'))
        return Group()
