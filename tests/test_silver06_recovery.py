"""Exercise notebook06 production definitions; no Spark or cloud execution.

The expression double models only the operators used by the reviewed predicate,
including NULL propagation. This is not a substitute for authorized Spark validation.
"""
import ast
import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace
import unittest

PATH = Path(__file__).resolve().parents[1] / 'notebooks/06_gtfs_realtime_silver.py'
SOURCE = PATH.read_text(encoding='utf-8')
TREE = ast.parse(SOURCE)
FINGERPRINT = 'f1e98d9ab7629000f335e0d341c3e4b018bfbe46ba92af06aece03f97ea73867'
KEY = ['feed_timestamp', 'entity_id', 'trip_id', 'stop_sequence', 'stop_id']


class Expr:
    def __init__(self, fn): self.fn = fn
    def cast(self, dtype):
        assert dtype == 'string'
        return Expr(lambda row: None if self.fn(row) is None else str(self.fn(row)))
    def __eq__(self, other):
        return Expr(lambda row: None if self.fn(row) is None else self.fn(row) == other)
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
             'validate_service_date_quality', 'write_reviewed_quarantine'}
    constants = {'REVIEWED_SERVICE_DATE_EXCEPTIONS', 'MAX_INVALID_SERVICE_DATE_RATE'}
    body = [node for node in TREE.body if
            (isinstance(node, ast.FunctionDef) and node.name in names) or
            (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in constants for t in node.targets))]
    scope = dict(F=Functions, hashlib=hashlib, trip_updates_dedup_key=KEY)
    exec(compile(ast.Module(body=body, type_ignores=[]), str(PATH), 'exec'), scope)
    return scope


class Frame:
    schema = 'unchanged-quarantine-schema'
    def __init__(self, keys, store): self.keys, self.store = list(keys), store
    @property
    def write(self): return Writer(self)
    def select(self, *names):
        assert list(names) == KEY
        return self
    def limit(self, n): return Frame(self.keys[:n], self.store)
    def collect(self): return list(self.keys)
    def count(self): return len(self.keys)


class Writer:
    def __init__(self, frame): self.frame = frame
    def format(self, value):
        assert value == 'delta'
        return self
    def mode(self, value):
        assert value == 'overwrite'
        return self
    def option(self, name, value):
        assert (name, value) == ('overwriteSchema', 'true')
        return self
    def saveAsTable(self, name):
        store = self.frame.store
        store['attempts'].append(name)
        if store.get('fail') == name: raise IOError('injected write failure')
        store[name] = self.frame


class Silver06RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.scope = production_scope()
        self.registry = self.scope['REVIEWED_SERVICE_DATE_EXCEPTIONS']
        entry = self.registry[1794498556]
        self.keys = self.scope['reviewed_exception_keys'](1794498556, entry)
        self.rows = [dict(zip(KEY, k), vehicle_id='7740', direction_id='0',
                          route_id='', start_time='', _service_date_raw='', service_date=None) for k in self.keys]

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

    def setup_store(self):
        store = {'attempts': []}
        spark = SimpleNamespace(table=lambda name: store[name],
                                sql=lambda query: SimpleNamespace(first=lambda: {'version':1}),
                                read=SimpleNamespace(option=lambda *args: SimpleNamespace(table=lambda name:store[name])))
        self.scope['spark'] = spark
        return store

    def test_quarantine_readback_checks_actual_identity_not_just_count(self):
        store = self.setup_store()
        frame = Frame(self.keys, store)
        wrong = list(self.keys); wrong[0] = tuple(['wrong']+list(wrong[0][1:]))
        for saved_keys in [wrong, self.keys[:-1], self.keys + [self.keys[0]]]:
            with self.subTest(saved_keys=len(saved_keys)):
                self.scope['spark'].table = lambda name: Frame(saved_keys, store)
                with self.assertRaisesRegex(ValueError, 'read-back identity'):
                    self.scope['write_reviewed_quarantine'](frame, self.keys)
        wrong_schema = Frame(self.keys, store)
        wrong_schema.schema = 'unexpected'
        self.scope['spark'].table = lambda name: wrong_schema
        with self.assertRaisesRegex(ValueError, 'read-back schema'):
            self.scope['write_reviewed_quarantine'](frame, self.keys)

    def test_quarantine_duplicate_or_foreign_keys_fail_before_write(self):
        store = self.setup_store()
        for keys in [self.keys+[self.keys[0]], [('foreign',)*5]]:
            with self.subTest(keys=keys[:1]), self.assertRaises(ValueError):
                self.scope['write_reviewed_quarantine'](Frame(keys,store),keys)
        self.assertEqual(store['attempts'], [])

    def test_actual_persistence_tail_is_gated_and_observations_are_last(self):
        tail = SOURCE[SOURCE.index('if not ALLOW_SILVER_OVERWRITE:'):]
        for failed in [None,'trip_updates_quarantine','trip_updates','vehicle_positions','realtime_trip_stop_observations']:
            with self.subTest(failed=failed):
                store = self.setup_store()
                outputs = {name:Frame([('new',)],store) for name in ['trip_updates','vehicle_positions','realtime_trip_stop_observations']}
                for name in outputs: store['rome_transport.silver.'+name] = Frame([('old',)],store)
                self.scope.update(ALLOW_SILVER_OVERWRITE=True,quarantine_df=Frame(self.keys,store),quarantine_keys=self.keys,
                                  outputs=outputs,expected_counts={name:1 for name in outputs},source_versions={},print=lambda *args:None)
                if failed:store['fail']='rome_transport.silver.'+failed
                if failed:
                    with self.assertRaises(IOError):exec(compile(tail,str(PATH),'exec'),self.scope)
                    self.assertEqual(store['rome_transport.silver.realtime_trip_stop_observations'].keys,[('old',)])
                else:
                    exec(compile(tail,str(PATH),'exec'),self.scope)
                    self.assertEqual(store['attempts'],['rome_transport.silver.'+n for n in ['trip_updates_quarantine','trip_updates','vehicle_positions','realtime_trip_stop_observations']])
                store['attempts'].clear();self.scope['ALLOW_SILVER_OVERWRITE']=False
                with self.assertRaises(RuntimeError):exec(compile(tail,str(PATH),'exec'),self.scope)
                self.assertEqual(store['attempts'],[])

    def test_repeat_quarantine_overwrite_preserves_identical_keys(self):
        store = self.setup_store(); frame = Frame(self.keys,store)
        for _ in range(2):self.scope['write_reviewed_quarantine'](frame,self.keys)
        self.assertEqual(store['rome_transport.silver.trip_updates_quarantine'].keys,self.keys)

    def test_no_bronze_writes_and_quality_diagnostics_exclude_quarantine(self):
        update = next(node for node in ast.walk(TREE) if isinstance(node, ast.Expr)
                      and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
                      and isinstance(node.value.func.value, ast.Name)
                      and node.value.func.value.id == 'service_date_quality' and node.value.func.attr == 'update')
        scope = dict(date_metrics={'invalid':1,'repaired':0}, quarantine_count=32,
                     silver_filtered_count=25333,
                     service_date_quality=self.scope['validate_service_date_quality'](25366,1,0,1/25366))
        exec(compile(ast.Module(body=[update],type_ignores=[]),str(PATH),'exec'), scope)
        self.assertEqual(scope['service_date_quality']['valid_rows'],25333)
        self.assertEqual(scope['service_date_quality']['excluded_rows'],33)
        self.assertEqual(scope['service_date_quality']['reviewed_quarantine_rows'],32)
        self.assertNotIn('saveAsTable("rome_transport.bronze', SOURCE)
        self.assertLess(SOURCE.index('validate_service_date_conservation(\n'),SOURCE.index('if not ALLOW_SILVER_OVERWRITE:'))
        self.assertEqual(self.scope['MAX_INVALID_SERVICE_DATE_RATE'],.001)


if __name__ == '__main__':unittest.main()
