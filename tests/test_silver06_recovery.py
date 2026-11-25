"""Exercise notebook06 production definitions; no Spark or cloud execution.

The expression double models only the operators used by the reviewed predicate,
including NULL propagation. This is not a substitute for authorized Spark validation.
The cumulative-gate tests drive the production helpers over a pure-Python model of
Bronze -> Silver/quarantine-ledger; they prove the arithmetic and write ordering, not Spark.
"""
import ast
import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace
import unittest
import uuid

PATH = Path(__file__).resolve().parents[1] / 'notebooks/06_gtfs_realtime_silver.py'
SOURCE = PATH.read_text(encoding='utf-8')
TREE = ast.parse(SOURCE)
FINGERPRINT = 'f1e98d9ab7629000f335e0d341c3e4b018bfbe46ba92af06aece03f97ea73867'
KEY = ['feed_timestamp', 'entity_id', 'trip_id', 'stop_sequence', 'stop_id']
QUARANTINE = 'rome_transport.silver.trip_updates_quarantine'


class Expr:
    def __init__(self, fn): self.fn = fn
    def cast(self, dtype):
        assert dtype == 'string'
        return Expr(lambda row: None if self.fn(row) is None else str(self.fn(row)))
    def __eq__(self, other):
        return Expr(lambda row: None if self.fn(row) is None else self.fn(row) == other)
    def __ge__(self, other): return Expr(lambda row: None)
    def __and__(self, other):
        def evaluate(row):
            a, b = self.fn(row), other.fn(row)
            return False if a is False or b is False else None if a is None or b is None else True
        return Expr(evaluate)
    def __or__(self, other):
        def evaluate(row):
            a, b = self.fn(row), other.fn(row)
            return True if a is True or b is True else None if a is None or b is None else False
        return Expr(evaluate)
    def isNull(self): return Expr(lambda row: self.fn(row) is None)
    def isin(self, values): return Expr(lambda row: None if self.fn(row) is None else self.fn(row) in values)


class Functions:
    col = staticmethod(lambda name: Expr(lambda row: row.get(name)))
    lit = staticmethod(lambda value: Expr(lambda row: value))
    @staticmethod
    def coalesce(*expressions):
        return Expr(lambda row: next((e.fn(row) for e in expressions if e.fn(row) is not None), None))
    @staticmethod
    def concat_ws(separator, *expressions):
        return Expr(lambda row: separator.join(str(e.fn(row)) for e in expressions if e.fn(row) is not None))


def production_scope():
    names = {'reviewed_exception_keys', 'validate_reviewed_service_date_registry',
             'reviewed_service_date_exception_expr', 'validate_service_date_conservation',
             'validate_service_date_quality', 'find_write_version', 'new_write_token',
             'tagged_write_version', 'cumulative_gate_counts', 'require_ledger_ready',
             'reviewed_keys_of', 'write_quarantine_ledger'}
    constants = {'REVIEWED_SERVICE_DATE_EXCEPTIONS', 'MAX_INVALID_SERVICE_DATE_RATE',
                 'QUARANTINE_TABLE', 'LEDGER_PROPERTY', 'LEDGER_VERSION'}
    body = [node for node in TREE.body if
            (isinstance(node, ast.FunctionDef) and node.name in names) or
            (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in constants for t in node.targets))]
    found = {n.name for n in body if isinstance(n, ast.FunctionDef)}
    assert found == names, names - found
    scope = dict(F=Functions, hashlib=hashlib, uuid=uuid, trip_updates_dedup_key=KEY)
    exec(compile(ast.Module(body=body, type_ignores=[]), str(PATH), 'exec'), scope)
    return scope


class Frame:
    schema = 'unchanged-quarantine-schema'
    def __init__(self, keys, store): self.keys, self.store = list(keys), store
    @property
    def write(self): return Writer(self)
    def filter(self, expression): return self
    def select(self, *names):
        assert list(names) == KEY
        return self
    def limit(self, n): return Frame(self.keys[:n], self.store)
    def collect(self): return list(self.keys)
    def count(self): return len(self.keys)


class Writer:
    def __init__(self, frame): self.frame, self.options = frame, {}
    def format(self, value):
        assert value == 'delta'
        return self
    def mode(self, value):
        assert value == 'overwrite'
        return self
    def option(self, name, value):
        assert name in ('overwriteSchema', 'userMetadata'), name
        if name == 'overwriteSchema': assert value == 'true'
        self.options[name] = value
        return self
    def saveAsTable(self, name):
        store = self.frame.store
        store['attempts'].append(name)
        if store.get('fail') == name: raise IOError('injected write failure')
        store[name] = self.frame
        history = store['history'].setdefault(name, [])
        version = history[0]['version'] + 1 if history else 0
        # an auto OPTIMIZE commit lands right after the real write and must never be picked
        history[:0] = [{'version': version + 1, 'userMetadata': None},
                       {'version': version, 'userMetadata': self.options.get('userMetadata')}]


def make_spark(store):
    def sql(query):
        words = query.split()
        if query.startswith('DESCRIBE HISTORY'):
            assert query.endswith('LIMIT 50')
            return SimpleNamespace(collect=lambda: list(store['history'].get(words[2], [])))
        if query.startswith('ALTER TABLE'):
            table = words[2]
            store['events'].append(('unset' if 'UNSET' in query else 'set', table))
            props = store['props'].setdefault(table, {})
            if 'UNSET' in query: props.pop('rmi.quarantine_ledger', None)
            else: props['rmi.quarantine_ledger'] = 'v1'
            return None
        raise AssertionError(query)
    return SimpleNamespace(
        table=lambda name: store[name], sql=sql,
        catalog=SimpleNamespace(tableExists=lambda name: name in store),
        read=SimpleNamespace(option=lambda *args: SimpleNamespace(table=lambda name: store[name])))


def new_store():
    return {'attempts': [], 'history': {}, 'props': {}, 'events': []}


# ---- pure-Python model of Bronze -> Silver / quarantine ledger (dedup key reduced to ts+entity) ----
REVIEWED = {(100, 'e3')}


def dedup(rows):
    latest = {}
    for row in sorted(rows, key=lambda r: r['ing']): latest[(row['ts'], row['entity'])] = row
    return list(latest.values())


def make_bronze(snapshots=range(100, 2100, 100), per=20, bad=(), superseded=()):
    rows = []
    for ts in snapshots:
        for i in range(per):
            invalid = (ts, f'e{i}') in set(bad)
            rows.append(dict(ts=ts, entity=f'e{i}', date=None if invalid else '20260101', ing=ts + 5))
            if (ts, f'e{i}') in set(superseded):   # invalid first copy, valid later copy: dedup keeps the valid one
                rows[-1]['date'] = None
                rows.append(dict(ts=ts, entity=f'e{i}', date='20260101', ing=ts + 9))
    return rows


def full_truth(bronze):
    d = dedup(bronze)
    ledger = [r for r in d if r['date'] is None]
    unreviewed = [r for r in ledger if (r['ts'], r['entity']) not in REVIEWED]
    return ({'total': len(d), 'invalid': len(unreviewed)},
            sorted((r['ts'], r['entity']) for r in ledger),
            sorted((r['ts'], r['entity']) for r in d if r['date'] is not None))


class Model:
    """Incremental store: quarantine ledger first, Silver last, both replaced with ts >= start."""
    def __init__(self, scope): self.scope, self.silver, self.ledger = scope, [], []

    def run(self, bronze, start, crash_after_ledger=False):
        window = dedup([r for r in bronze if r['ts'] >= start])
        invalid = [r for r in window if r['date'] is None]
        valid = [r for r in window if r['date'] is not None]
        reviewed_window = [r for r in invalid if (r['ts'], r['entity']) in REVIEWED]
        counts = self.scope['cumulative_gate_counts'](
            sum(r['ts'] < start for r in self.silver),
            sum(r['ts'] < start for r in self.ledger),
            sum(r['ts'] < start and (r['ts'], r['entity']) in REVIEWED for r in self.ledger),
            len(window), len(invalid) - len(reviewed_window))
        self.scope['validate_service_date_quality'](counts['total'], counts['invalid'], 0, 0.0)  # may raise: no writes
        self.ledger = [r for r in self.ledger if r['ts'] < start] + invalid
        if crash_after_ledger: return counts
        self.silver = [r for r in self.silver if r['ts'] < start] + valid
        return counts

    def state(self):
        return (sorted((r['ts'], r['entity']) for r in self.ledger),
                sorted((r['ts'], r['entity']) for r in self.silver))


class Silver06RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.scope = production_scope()
        self.registry = self.scope['REVIEWED_SERVICE_DATE_EXCEPTIONS']
        entry = self.registry[1794498556]
        self.keys = self.scope['reviewed_exception_keys'](1794498556, entry)
        self.rows = [dict(zip(KEY, k), vehicle_id='7740', direction_id='0',
                           route_id='', start_time='', _service_date_raw='', service_date=None) for k in self.keys]

    def setup_store(self):
        store = new_store()
        self.scope['spark'] = make_spark(store)
        return store

    def test_exact_production_registry_and_real_fingerprint(self):
        self.scope['validate_reviewed_service_date_registry'](self.registry)
        self.assertEqual(len(self.keys), 32)
        self.assertEqual(len(set(self.keys)), 32)
        self.assertEqual(hashlib.sha256('\n'.join('|'.join(k) for k in sorted(self.keys)).encode()).hexdigest(), FINGERPRINT)
        self.assertEqual({int(r['stop_sequence']) for r in self.rows}, set(range(2, 34)))

    def test_production_expression_accepts_only_reviewed_32(self):
        expression = self.scope['reviewed_service_date_exception_expr'](self.registry)
        self.assertTrue(all(expression.fn(row) is True for row in self.rows))
        stray = dict(self.rows[0], entity_id='1308', trip_id='101618808A808A', vehicle_id='7715', stop_sequence='34', stop_id='79478')
        self.assertIs(expression.fn(stray), False)

    def test_each_changed_identity_and_profile_is_unreviewed(self):
        expression = self.scope['reviewed_service_date_exception_expr'](self.registry)
        for field, value in [('feed_timestamp','1794498557'),('entity_id','1308'),('trip_id','OTHER'),
                             ('stop_sequence','99'),('stop_id','UNKNOWN'),('vehicle_id','9999'),
                             ('direction_id','1'),('route_id','R9'),('start_time','12:00:00'),
                             ('_service_date_raw','20260230'),('service_date','2026-11-12')]:
            with self.subTest(field=field): self.assertIs(expression.fn(dict(self.rows[0], **{field:value})), False)

    def test_null_and_blank_are_not_silently_equated(self):
        expression = self.scope['reviewed_service_date_exception_expr'](self.registry)
        for field in KEY + ['vehicle_id','direction_id','route_id','start_time','_service_date_raw']:
            with self.subTest(field=field): self.assertIs(expression.fn(dict(self.rows[0], **{field:None})), False)
        self.assertIs(expression.fn(dict(self.rows[0], _service_date_raw=' ')), False)

    def test_corrupt_registry_blocks_before_expression(self):
        for changes in [dict(n=33), dict(sha256='0'*64), dict(stop_keys=self.registry[1794498556]['stop_keys'][:-1])]:
            registry = copy.deepcopy(self.registry)
            registry[1794498556].update(changes)
            with self.subTest(changes=list(changes)), self.assertRaises(ValueError):
                self.scope['reviewed_service_date_exception_expr'](registry)

    def test_actual_gate_preserves_snapshot_threshold_and_stray(self):
        validate = self.scope['validate_service_date_quality']
        with self.assertRaises(ValueError): validate(92647910, 34, 0, 33/25366)
        self.assertEqual(validate(92647910, 2, 0, 1/25366)['excluded_rows'], 2)
        validate(1000, 1, 0, .001)
        with self.assertRaises(ValueError): validate(1000, 2, 0, .002)

    def test_production_conservation_rejects_loss_double_counts_and_negatives(self):
        validate = self.scope['validate_service_date_conservation']
        validate(25366, 25333, 32, 1)
        for values in [(25366,25332,32,1),(25366,25334,32,1),(25366,25366,-1,1)]:
            with self.subTest(values=values), self.assertRaises(ValueError): validate(*values)


    # ---- ported persistence tests (ledger writer) ----
    def test_quarantine_readback_checks_actual_identity_not_just_count(self):
        store = self.setup_store()
        frame = Frame(self.keys, store)
        wrong = list(self.keys); wrong[0] = tuple(['wrong'] + list(wrong[0][1:]))
        for saved_keys in [wrong, self.keys[:-1] + [self.keys[0]]]:
            with self.subTest(saved_keys=len(saved_keys)):
                self.scope['spark'].read = SimpleNamespace(option=lambda *a: SimpleNamespace(table=lambda n: Frame(saved_keys, store)))
                with self.assertRaisesRegex(ValueError, 'read-back identity'):
                    self.scope['write_quarantine_ledger'](frame, self.keys, 32)
        self.assertNotIn(('set', QUARANTINE), store['events'])
        wrong_schema = Frame(self.keys, store); wrong_schema.schema = 'unexpected'
        self.scope['spark'].read = SimpleNamespace(option=lambda *a: SimpleNamespace(table=lambda n: wrong_schema))
        with self.assertRaisesRegex(ValueError, 'read-back schema'):
            self.scope['write_quarantine_ledger'](frame, self.keys, 32)
        self.scope['spark'].read = SimpleNamespace(option=lambda *a: SimpleNamespace(table=lambda n: Frame(self.keys[:-1], store)))
        with self.assertRaisesRegex(ValueError, 'read-back count'):
            self.scope['write_quarantine_ledger'](frame, self.keys, 32)

    def test_quarantine_duplicate_or_foreign_keys_fail_before_write(self):
        store = self.setup_store()
        for keys in [self.keys + [self.keys[0]], [('foreign',) * 5]]:
            with self.subTest(keys=keys[:1]), self.assertRaises(ValueError):
                self.scope['write_quarantine_ledger'](Frame(keys, store), keys, len(keys))
        self.assertEqual(store['attempts'], [])

    def test_actual_persistence_tail_is_gated_and_observations_are_last(self):
        tail = SOURCE[SOURCE.index('if not ALLOW_SILVER_OVERWRITE:'):]
        names = ['trip_updates', 'vehicle_positions', 'realtime_trip_stop_observations']
        for failed in [None, 'trip_updates_quarantine'] + names:
            with self.subTest(failed=failed):
                store = self.setup_store()
                outputs = {n: Frame([('new',)], store) for n in names}
                for n in names: store['rome_transport.silver.' + n] = Frame([('old',)], store)
                self.scope.update(ALLOW_SILVER_OVERWRITE=True, quarantine_df=Frame(self.keys, store),
                                  quarantine_keys=self.keys, quarantine_count=32, outputs=outputs,
                                  expected_counts={n: 1 for n in names}, source_versions={}, print=lambda *a: None)
                if failed: store['fail'] = 'rome_transport.silver.' + failed
                if failed:
                    with self.assertRaises(IOError): exec(compile(tail, str(PATH), 'exec'), self.scope)
                    self.assertEqual(store['rome_transport.silver.realtime_trip_stop_observations'].keys, [('old',)])
                else:
                    exec(compile(tail, str(PATH), 'exec'), self.scope)
                    self.assertEqual(store['attempts'], ['rome_transport.silver.' + n for n in ['trip_updates_quarantine'] + names])
                store['attempts'].clear(); self.scope['ALLOW_SILVER_OVERWRITE'] = False
                with self.assertRaises(RuntimeError): exec(compile(tail, str(PATH), 'exec'), self.scope)
                self.assertEqual(store['attempts'], [])

    def test_repeat_quarantine_overwrite_preserves_identical_keys(self):
        store = self.setup_store(); frame = Frame(self.keys, store)
        versions = [self.scope['write_quarantine_ledger'](frame, self.keys, 32) for _ in range(2)]
        self.assertEqual(store[QUARANTINE].keys, self.keys)
        self.assertEqual(versions, [0, 2])  # each write found by its own token, never the OPTIMIZE commit

    def test_diagnostics_report_ledger_and_cumulative_counts(self):
        update = next(node for node in ast.walk(TREE) if isinstance(node, ast.Expr)
                      and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
                      and isinstance(node.value.func.value, ast.Name)
                      and node.value.func.value.id == 'service_date_quality' and node.value.func.attr == 'update')
        scope = dict(date_metrics={'invalid': 1, 'repaired': 0}, quarantine_count=33, reviewed_count=32,
                     gate_counts={'total': 92647000, 'invalid': 2}, silver_filtered_count=25333,
                     service_date_quality=self.scope['validate_service_date_quality'](25366, 1, 0, 1 / 25366))
        exec(compile(ast.Module(body=[update], type_ignores=[]), str(PATH), 'exec'), scope)
        q = scope['service_date_quality']
        self.assertEqual((q['valid_rows'], q['excluded_rows'], q['reviewed_quarantine_rows']), (25333, 33, 32))
        self.assertEqual((q['cumulative_total_rows'], q['cumulative_invalid_rows']), (92647000, 2))
        self.assertNotIn('saveAsTable("rome_transport.bronze', SOURCE)
        self.assertLess(SOURCE.index('validate_service_date_conservation(\n'), SOURCE.index('if not ALLOW_SILVER_OVERWRITE:'))
        self.assertEqual(self.scope['MAX_INVALID_SERVICE_DATE_RATE'], .001)

    # ---- write identification (requirement 8) ----
    def test_find_write_version_skips_optimize_and_fails_closed(self):
        find = self.scope['find_write_version']
        history = [{'version': 9, 'userMetadata': None}, {'version': 8, 'userMetadata': 'tok'},
                   {'version': 7, 'userMetadata': 'other'}]
        self.assertEqual(find(history, 'tok'), 8)
        for rows in [[], history[:1], history + [{'version': 6, 'userMetadata': 'tok'}]]:
            with self.subTest(n=len(rows)), self.assertRaisesRegex(ValueError, 'not uniquely identified'):
                find(rows, 'tok')
        self.assertNotEqual(self.scope['new_write_token']('x'), self.scope['new_write_token']('x'))

    # ---- gate arithmetic ----
    def test_cumulative_counts_subtract_reviewed_and_reject_bad_input(self):
        gate = self.scope['cumulative_gate_counts']
        self.assertEqual(gate(92647000, 34, 32, 910, 0), {'total': 92647944, 'invalid': 2})
        self.assertEqual(gate(0, 0, 0, 1000, 1), {'total': 1000, 'invalid': 1})
        for values in [(-1, 0, 0, 1, 0), (1, 1, 2, 1, 0), (1, 0, 0, 1, 2), (True, 0, 0, 1, 0), (1.0, 0, 0, 1, 0)]:
            with self.subTest(values=values), self.assertRaises(ValueError): gate(*values)

    def test_cumulative_gate_fails_where_window_alone_would_pass(self):
        gate, validate = self.scope['cumulative_gate_counts'], self.scope['validate_service_date_quality']
        validate(100, 0, 0, 0.0)  # window alone is clean
        counts = gate(4000, 5, 0, 100, 0)
        with self.assertRaises(ValueError): validate(counts['total'], counts['invalid'], 0, 0.0)
        ok = gate(4000, 4, 0, 100, 0)  # 4 / 4104 <= 0.1%
        validate(ok['total'], ok['invalid'], 0, 0.0)

    def test_require_ledger_ready(self):
        ready = self.scope['require_ledger_ready']
        ready({'rmi.quarantine_ledger': 'v1'})
        for props in [{}, {'rmi.quarantine_ledger': 'v0'}]:
            with self.assertRaisesRegex(ValueError, 'not initialised'): ready(props)

    # ---- equivalence with full mode, idempotency, overlap, late arrivals, partial writes ----
    def bronze(self, **kw):
        return make_bronze(per=100, bad=[(100, 'e3'), (500, 'e1'), (1500, 'e2')] + kw.pop('extra', []), **kw)

    def test_incremental_gate_equals_full_over_overlapping_windows_and_reruns(self):
        bronze = self.bronze()
        truth, truth_ledger, truth_silver = full_truth(bronze)
        self.assertEqual(truth, {'total': 2000, 'invalid': 2})  # exactly 0.1%: passes
        model = Model(self.scope)
        for start in [0, 1000, 1500, 1800, 1900, 1500, 0, 2000, 1000]:  # overlaps, reruns, going backwards
            counts = model.run(bronze, start)
            with self.subTest(start=start):
                self.assertEqual(counts, truth)
                self.assertEqual(model.state(), (truth_ledger, truth_silver))

    def test_rerun_is_idempotent_and_does_not_double_count(self):
        bronze = self.bronze(); model = Model(self.scope); model.run(bronze, 0)
        first = (model.run(bronze, 1200), model.state())
        for _ in range(3): self.assertEqual((model.run(bronze, 1200), model.state()), first)
        self.assertEqual(len(model.ledger), len({(r['ts'], r['entity']) for r in model.ledger}))

    def test_late_arrival_and_superseded_copy(self):
        bronze = self.bronze(superseded=[(1700, 'e9')]); model = Model(self.scope); model.run(bronze, 0)
        truth, ledger, silver = full_truth(bronze)
        self.assertNotIn((1700, 'e9'), ledger)  # invalid first copy superseded by a valid later one
        late = bronze + [dict(ts=1500, entity='e2', date='20260101', ing=9999)]  # late valid copy repairs a ledger row
        new_truth, new_ledger, new_silver = full_truth(late)
        self.assertEqual(new_truth['invalid'], truth['invalid'] - 1)
        self.assertEqual(model.run(late, 1500), new_truth)
        self.assertEqual(model.state(), (new_ledger, new_silver))
        late += [dict(ts=1600, entity='e5', date=None, ing=10000)]  # late invalid copy of a new key
        self.assertEqual(model.run(late, 1500), full_truth(late)[0])

    def test_cumulative_gate_blocks_before_any_write_when_history_pushes_it_over(self):
        bronze = self.bronze(); model = Model(self.scope); model.run(bronze, 0)
        before = model.state()
        worse = bronze + [dict(ts=2000, entity='e0', date=None, ing=7777), dict(ts=300, entity='e0', date=None, ing=7778)]
        with self.assertRaises(ValueError): model.run(worse, 1900)  # history (2) + window (1) = 3 / 2000 > 0.1%
        self.assertEqual(model.state(), before)

    def test_partial_write_is_repaired_by_rerunning_same_or_earlier_window(self):
        bronze = self.bronze()
        late_valid = bronze + [dict(ts=1500, entity='e2', date='20260101', ing=8000)]  # repairs a ledger row
        truth, ledger, silver = full_truth(late_valid)
        for repair_start in [1500, 1000, 0]:
            with self.subTest(repair_start=repair_start):
                model = Model(self.scope); model.run(bronze, 0)
                model.run(late_valid, 1500, crash_after_ledger=True)  # crash between the two Delta writes
                self.assertNotEqual(model.state(), (ledger, silver))  # Silver still lacks the repaired row
                self.assertNotIn((1500, 'e2'), model.state()[0])      # ledger is never behind Silver
                self.assertEqual(model.run(late_valid, repair_start), truth)
                self.assertEqual(model.state(), (ledger, silver))

    def test_reviewed_rows_survive_every_window_and_stay_out_of_the_gate(self):
        bronze = self.bronze(); model = Model(self.scope)
        for start in [0, 100, 1500, 0]:
            counts = model.run(bronze, start)
            self.assertIn((100, 'e3'), model.state()[0])  # reviewed row stays in the ledger
            self.assertEqual(counts['invalid'], 2)        # ...but not in the numerator
        self.assertEqual(hashlib.sha256('\n'.join('|'.join(k) for k in sorted(self.keys)).encode()).hexdigest(), FINGERPRINT)

    # ---- marker lifecycle ----
    def test_marker_is_withdrawn_before_full_write_and_restored_only_after_verification(self):
        store = self.setup_store(); frame = Frame(self.keys, store)
        self.scope['write_quarantine_ledger'](frame, self.keys, 32)
        self.assertEqual(store['events'], [('set', QUARANTINE)])  # first-ever write: no table to unset
        store['events'].clear()
        self.scope['write_quarantine_ledger'](frame, self.keys, 32)
        self.assertEqual(store['events'], [('unset', QUARANTINE), ('set', QUARANTINE)])
        store['events'].clear(); store['fail'] = QUARANTINE
        with self.assertRaises(IOError): self.scope['write_quarantine_ledger'](frame, self.keys, 32)
        self.assertEqual(store['events'], [('unset', QUARANTINE)])
        self.assertNotIn('rmi.quarantine_ledger', store['props'][QUARANTINE])  # incremental stays blocked

    def test_incremental_window_write_does_not_touch_marker(self):
        store = self.setup_store(); store[QUARANTINE] = Frame(self.keys, store)
        calls = []
        self.scope['replace_window'] = lambda f, dest, start, n: calls.append((dest, start, n)) or 7
        self.assertEqual(self.scope['write_quarantine_ledger'](Frame(self.keys, store), self.keys, 32, 1500), 7)
        self.assertEqual(calls, [(QUARANTINE, 1500, 32)])
        self.assertEqual(store['events'], [])


if __name__ == '__main__': unittest.main()
