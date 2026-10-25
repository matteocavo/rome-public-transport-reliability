"""Execute shared production expressions and the pinned old 08 oracle offline."""
import ast
import hashlib
import json
from datetime import date, datetime
from functools import reduce
from pathlib import Path
import unittest

from feature_frame import Frame, Functions, Window, ROME

ROOT = Path(__file__).resolve().parents[1]
HASH = '11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5'


def load_common():
    path = ROOT / 'notebooks/08_common_feature_preparation.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    nodes = [n for n in tree.body if not isinstance(n, (ast.Import, ast.ImportFrom))]
    scope = dict(F=Functions, Window=Window, json=json, hashlib=hashlib, reduce=reduce)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), scope)
    return scope


def fixture():
    base = dict(feed_timestamp=100, feed_datetime=datetime.fromtimestamp(100, ROME), service_date=date(2026, 9, 25),
        entity_id='e', trip_id='t', vehicle_id='v', start_time='08:00:00', route_id='r', route_short_name='R',
        route_type=3, agency_id='a', direction_id=0, stop_id='s1', stop_sequence=1,
        arrival_delay_seconds=10, departure_delay_seconds=None, arrival_seconds=100,
        stop_lat=41.9, stop_lon=12.5, vehicle_speed=None, vehicle_bearing=None, vehicle_odometer=None,
        current_stop_sequence=1, current_status='IN_TRANSIT_TO',
        vehicle_datetime=datetime.fromtimestamp(90, ROME), position_time_diff_seconds=10,
        active_alert_flag=False, active_alert_count=0, active_detour_flag=False, active_construction_flag=False,
        alert_causes=[], alert_effects=[], active_alert_ids=[], alert_snapshot_available_flag=True,
        matched_alert_feed_timestamp=90, alert_snapshot_age_seconds=10, vehicle_current_stop_id='physical-stop')
    rows = [base,
        dict(base, stop_id='s2', stop_sequence=2, arrival_delay_seconds=None, arrival_seconds=160),
        dict(base, stop_id='s4', stop_sequence=4, arrival_delay_seconds=30, arrival_seconds=220),
        dict(base, feed_timestamp=101, feed_datetime=datetime.fromtimestamp(101, ROME), arrival_delay_seconds=50),
        dict(base, feed_timestamp=101, feed_datetime=datetime.fromtimestamp(101, ROME), stop_id='s2',
             stop_sequence=2, arrival_delay_seconds=None, arrival_seconds=160),
        dict(base, feed_timestamp=100, entity_id='other', trip_id='other', vehicle_id='other', arrival_delay_seconds=90),
        dict(base, feed_timestamp=102, feed_datetime=datetime.fromtimestamp(102, ROME),
             arrival_delay_seconds=99999)]
    types = {k: ('int' if k in ['departure_delay_seconds'] else 'double' if k in ['vehicle_speed','vehicle_bearing','vehicle_odometer']
                 else 'date' if k == 'service_date' else 'bigint' if k in ['feed_timestamp','position_time_diff_seconds','matched_alert_feed_timestamp','alert_snapshot_age_seconds']
                 else None) for k in base}
    frame = Frame(rows)
    frame.types.update({k:v for k,v in types.items() if v})
    return frame


class CurrentScoringTest(unittest.TestCase):
    def setUp(self):
        self.c = load_common()
        self.source = fixture()

    def test_frozen_contract_counts_order_and_hash(self):
        self.assertEqual(self.c['verify_feature_contract'](), HASH)
        self.assertEqual(len(self.c['NUMERICAL_FEATURES']), 42)
        self.assertEqual(self.c['CATEGORICAL_FEATURES'], ['route_id','route_type','direction_id','agency_id','current_status'])
        self.assertEqual(len(self.c['MODEL_FEATURES']), 47)
        # The model notebooks are unchanged, and their candidate order agrees with the frozen selection.
        for name in ['10_model_training.py','11_model_tuning_and_selection.py','12_model_registration_and_scoring.py']:
            tree = ast.parse((ROOT/'notebooks'/name).read_text(encoding='utf-8'))
            n = next(n for n in tree.body if isinstance(n, ast.Assign)
                     and any(isinstance(t, ast.Name) and t.id=='candidate_numeric_features' for t in n.targets))
            candidates = eval(compile(ast.Expression(n.value),'candidates','eval'))
            self.assertEqual([x for x in candidates if x != 'trip_progress_ratio'], self.c['NUMERICAL_FEATURES'])

    def test_old_training_output_parity_all_values_types_and_rows(self):
        scope = dict(self.c)
        path = ROOT/'tests/fixtures/feature_preparation_before_pr3.py'
        exec(compile(path.read_text(encoding='utf-8'),str(path),'exec'),scope)
        old = scope['baseline_training_features'](self.source, 7)
        # Execute the real new training adapter from 08, including provisional audit fields.
        text = (ROOT/'notebooks/08_feature_engineering.py').read_text(encoding='utf-8')
        adapter = text[text.index('features = prepare_common_features'):text.index('# MAGIC ## Final Feature Selection')]
        scope.update(observations=self.source, SOURCE_VERSION=7, major_delay_threshold_seconds=300,
                     stop_order=Window.partitionBy(*self.c['TRIP_SNAPSHOT_KEY']).orderBy('stop_sequence'))
        exec(compile(adapter,'actual_08_training_adapter','exec'),scope)
        new = scope['features']
        self.assertEqual(old.count(), new.count())
        self.assertEqual(set(old.columns),set(new.columns))
        self.assertEqual(old.types,new.types)
        self.assertEqual(old.rows,new.rows)
        # Independently checked fixture values: ties excluded, sparse next stop kept, lag is preceding.
        self.assertEqual(new.rows[0]['historical_route_mean_delay'],None)
        self.assertEqual(new.rows[3]['historical_route_mean_delay'],(10+30+90)/3)
        self.assertEqual(new.rows[3]['historical_route_observation_count'],4)
        self.assertEqual(new.rows[1]['target_next_stop_sequence'],4)
        self.assertEqual(new.rows[2]['lag_1_arrival_delay_seconds'],None)
        self.assertEqual(new.rows[2]['lag_2_arrival_delay_seconds'],10)
        # Final persisted projection and eligibility remain the old 08 expressions.
        def training_rows(frame):
            return frame.filter(Functions.col('target_next_stop_id').isNotNull()
                & (Functions.col('target_next_stop_sequence') > Functions.col('stop_sequence'))
                & Functions.col('target_next_stop_delay_seconds').isNotNull())
        self.assertEqual(training_rows(old).rows,training_rows(new).rows)

    def prepare(self, source=None):
        return self.c['prepare_scoring_dataset'](source or self.source, 7, 101,
            datetime.fromtimestamp(103, ROME),'fixture-batch')

    def test_scoring_without_labels_keeps_distinct_next_stop_and_null_next_delay(self):
        result = self.prepare()
        self.assertEqual(result.count(),1)
        row=result.rows[0]
        self.assertEqual((row['stop_id'],row['stop_sequence']),('s1',1))
        self.assertEqual((row['target_next_stop_id'],row['target_next_stop_sequence']),('s2',2))
        self.assertEqual(row['vehicle_current_stop_id'],'physical-stop')
        self.assertNotIn('target_next_stop_delay_seconds',result.columns)
        self.assertNotIn('target_realized_major_delay_flag',result.columns)
        self.assertEqual(row['feature_contract_hash'],HASH)
        self.assertEqual(row['source_silver_version'],7)
        self.assertEqual(result.columns,self.c['SCORING_COLUMNS'])
        self.assertEqual([result.types[x] for x in self.c['MODEL_FEATURES']],
                         [self.c['MODEL_RAW_TYPES'][x] for x in self.c['MODEL_FEATURES']])

    def test_future_rows_and_poison_labels_do_not_change_scoring_features(self):
        original=self.prepare()
        changed=Frame([dict(r,label_source_feed_timestamp=999999,target_realized_next_stop_delay_seconds=-999)
                       for r in self.source.rows],dict(self.source.types,label_source_feed_timestamp='bigint',
                           target_realized_next_stop_delay_seconds='int'))
        changed.rows[-1]['arrival_delay_seconds']=-123456
        self.assertEqual(original.rows,self.prepare(changed).rows)
        self.assertFalse(any(x.startswith(('label_','provisional_','future_')) for x in original.columns))
        self.assertFalse(any(x.startswith('target_') for x in self.c['MODEL_FEATURES']))

    def test_optional_null_values_allowed_no_new_imputation(self):
        result=self.prepare()
        self.assertIsNone(result.rows[0]['vehicle_speed'])
        self.assertIsNone(result.rows[0]['current_departure_delay_seconds'])
        self.assertEqual(result.rows[0]['missing_current_departure_delay'],1)
        types=dict(self.c['MODEL_RAW_TYPES']);self.c['validate_model_schema'](types)

    def test_missing_required_model_feature_and_incompatible_types_block(self):
        types=dict(self.c['MODEL_RAW_TYPES']);types.pop('feed_hour')
        with self.assertRaisesRegex(ValueError,'Missing model features'):self.c['validate_model_schema'](types)
        types=dict(self.c['MODEL_RAW_TYPES']);types['vehicle_speed']='array<double>'
        with self.assertRaisesRegex(ValueError,'Incompatible'):self.c['validate_model_schema'](types)
        types=dict(self.c['MODEL_RAW_TYPES']);types['route_id']='int'
        with self.assertRaisesRegex(ValueError,'Incompatible'):self.c['validate_model_schema'](types)
        with self.assertRaisesRegex(ValueError,'hash mismatch'):
            self.c['validate_model_schema'](self.c['MODEL_RAW_TYPES'],'changed')

    def test_ambiguous_source_or_incoherent_target_blocks(self):
        source=Frame(self.source.rows+[dict(self.source.rows[0])],self.source.types)
        with self.assertRaisesRegex(ValueError,'Ambiguous'):self.prepare(source)
        result=self.prepare();result.rows[0]['target_next_stop_sequence']=1
        with self.assertRaisesRegex(ValueError,'Inconsistent'):self.c['validate_scoring_rows'](result)

    def test_missing_source_keys_and_persisted_contract_mismatch_block(self):
        source=fixture();source.rows[0]['stop_id']=None
        with self.assertRaisesRegex(ValueError,'identifiers'):self.prepare(source)
        result=self.prepare();result.rows[0]['feature_contract_hash']='wrong'
        with self.assertRaisesRegex(ValueError,'hash mismatch'):self.c['validate_scoring_rows'](result)

    def test_trip_progress_ratio_is_technical_null_and_not_scored(self):
        frame=self.c['prepare_common_features'](self.source)
        self.assertTrue(all(r['trip_progress_ratio'] is None for r in frame.rows))
        self.assertNotIn('trip_progress_ratio',self.c['MODEL_FEATURES'])
        self.assertNotIn('trip_progress_ratio',self.prepare().columns)

    def test_causal_guards_and_latest_selection(self):
        source=Frame([dict(r,matched_alert_feed_timestamp=99999) for r in self.source.rows],self.source.types)
        with self.assertRaisesRegex(ValueError,'Alert'):self.prepare(source)
        result=self.prepare()
        self.assertEqual({r['feed_timestamp'] for r in result.rows},{101})
        self.assertEqual(result.rows[0]['historical_route_observation_count'],4)
        self.assertEqual(result.rows[0]['historical_route_mean_delay'],(10+30+90)/3)

    def test_staging_contract_has_no_outcomes_and_no_model_actions(self):
        allowed={'target_next_stop_id','target_next_stop_sequence','target_feed_timestamp','target_trip_id','target_service_date'}
        self.assertFalse(set(self.c['MODEL_FEATURES']) & set(self.c['MODEL_CONTRACT']['forbidden']))
        for name in self.c['SCORING_COLUMNS']:
            self.assertFalse(name.startswith(('label_','provisional_','future_')))
            self.assertTrue(not name.startswith('target_') or name in allowed)
        text=(ROOT/'notebooks/08_current_scoring_dataset.py').read_text(encoding='utf-8')
        self.assertIn('ALLOW_STAGING_OVERWRITE = False',text)
        self.assertNotIn('next_stop_delay_labeled',text)
        tree=ast.parse(text)
        self.assertFalse(any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)
                             and n.func.attr in ['fit','transform','log_model'] for n in ast.walk(tree)))


if __name__=='__main__': unittest.main()
