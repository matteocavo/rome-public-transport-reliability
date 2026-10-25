"""Persisted inference contract tests; no Spark, MLflow service, training or binary artifacts."""
import ast
import copy
import math
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch
from datetime import datetime

from test_current_scoring_contract import load_common, fixture
from feature_frame import Frame, Expr, Functions, expr, col, ROME

ROOT=Path(__file__).resolve().parents[1]
HASH='11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5'


def load_contract():
    scope=load_common()
    tree=ast.parse((ROOT/'notebooks/12_common_persisted_classifier.py').read_text(encoding='utf-8-sig'))
    nodes=[n for n in tree.body if not isinstance(n,(ast.Import,ast.ImportFrom))]
    scope.update(copy=copy,math=math,datetime=datetime)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'persisted_classifier','exec'),scope)
    return scope


class Stage:
    def __init__(self,**params):self.params=params
    def getOrDefault(self,key):return self.params[key]
    def getInputCols(self):return self.params["inputCols"]


def fake_fitted_pipeline(c):
    x=c['preprocessing_layout']()
    stages=[
        ('SQLTransformer',dict(statement=x['statement'])),
        ('ImputerModel',dict(inputCols=x['numeric_raw'],outputCols=x['numeric_imputed'],strategy='median',relativeError=0.001,missingValue=float('nan'))),
        ('StringIndexerModel',dict(inputCols=x['category_raw'],outputCols=x['category_indices'],handleInvalid='keep',stringOrderType='alphabetAsc')),
        ('OneHotEncoderModel',dict(inputCols=x['category_indices'],outputCols=x['category_vectors'],handleInvalid='keep',dropLast=False)),
        ('VectorAssembler',dict(inputCols=x['numeric_imputed']+x['category_vectors'],outputCol='features',handleInvalid='error')),
        ('StandardScalerModel',dict(inputCol='features',outputCol='scaled_features',withStd=True,withMean=False)),
        ('RandomForestClassificationModel',dict(featuresCol='features',labelCol=c['CLASSIFIER_LABEL'],probabilityCol='probability',predictionCol='prediction',rawPredictionCol='rawPrediction',**c['FINAL_CLASSIFIER_PARAMS'])),
    ]
    objects=[type(name,(Stage,),{})(**params) for name,params in stages]
    objects[2].labelsArray=[['r'],['3'],['0'],['a'],['IN_TRANSIT_TO']]
    objects[3].categorySizes=[2]*5
    objects[6].numClasses=2;objects[6].numFeatures=57
    class LoadedPipeline:
        def __init__(self):self.stages=objects;self.calls=0;self.positive_probability=0.45
        def fit(self,*args):raise AssertionError('Inference must never fit')
        def transform(self,df):
            self.calls+=1
            rows=[]
            for row in df.rows:
                # Test double follows fitted keep buckets. Production uses the native persisted Spark stages.
                for name,known in zip(c['CATEGORICAL_FEATURES'],objects[2].labelsArray):
                    value=None if row[name] is None else str(row[name])
                    if value not in known and objects[2].params['handleInvalid']!='keep':raise ValueError('unseen')
                probability=self.positive_probability
                rows.append(dict(row,probability=[1-probability,probability],features=[0.0]*57))
            return Frame(rows,dict(df.types,probability='array<double>',features='array<double>'))
    return LoadedPipeline()


def output_expressions():
    def exists(array,predicate):
        array=col(array)
        return Expr(lambda f,i:any(predicate(expr(v)).fn(f,i) for v in array.fn(f,i)), 'boolean')
    def aggregate(array,zero,combine):
        array=col(array)
        def evaluate(frame,index):
            value=expr(zero).fn(frame,index)
            for item in array.fn(frame,index):value=combine(expr(value),expr(item)).fn(frame,index)
            return value
        return Expr(evaluate)
    def dedup(frame):
        seen=set();rows=[]
        for row in frame.rows:
            key=repr(row)
            if key not in seen:rows.append(row);seen.add(key)
        return Frame(rows,frame.types)
    def except_all(left,right):
        remaining=list(right.rows);result=[]
        for row in left.rows:
            if row in remaining:remaining.remove(row)
            else:result.append(row)
        return Frame(result,left.types)
    return [patch.object(Expr,'__getitem__',lambda self,key:Expr(lambda f,i:self.fn(f,i)[key]),create=True),
        patch.object(Functions,'exists',staticmethod(exists),create=True),
        patch.object(Functions,'aggregate',staticmethod(aggregate),create=True),
        patch.object(Functions,'isnan',staticmethod(lambda x:Functions.unary(x,math.isnan,'boolean')),create=True),
        patch.object(Frame,'dropDuplicates',dedup,create=True),patch.object(Frame,'exceptAll',except_all,create=True)]


class PersistedClassifierTest(unittest.TestCase):
    def setUp(self):
        self.c=load_contract()
        self.pipeline=fake_fitted_pipeline(self.c)
        self.metadata=self.c['make_artifact_metadata']('new-fit-run',8,'2026-10-25T00:00:00+00:00',
            '2026-10-25T01:00:00+00:00','a'*40,57,[2]*5)

    def test_model_feature_parameters_and_target_are_unchanged(self):
        self.assertEqual(self.c['verify_feature_contract'](),HASH)
        self.assertEqual(len(self.c['MODEL_FEATURES']),47)
        self.assertEqual((self.c['TARGET_EVENT'],self.c['TARGET_SECONDS']),('next_stop_delay_ge_300s',300))
        old=ast.parse((ROOT/'notebooks/12_model_registration_and_scoring.py').read_text(encoding='utf-8'))
        node=next(n for n in old.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='FINAL_PARAMS' for t in n.targets))
        self.assertEqual(self.c['FINAL_CLASSIFIER_PARAMS'],eval(compile(ast.Expression(node.value),'oldparams','eval')))

    def test_class_one_probability_and_strict_threshold(self):
        for p,expected in [(0,0),(0.45,0),(0.45001,1),(1,1)]:
            self.assertEqual(self.c['probability_and_flag']([1-p,p],[0,1]),(p,expected))
        for labels in [[1,0],[0,2],None]:
            with self.assertRaises(ValueError):self.c['class_one_index'](labels)
        for probabilities in [[0.2,0.9],[float('nan'),0.5],[0.5],[-0.1,1.1]]:
            with self.assertRaises(ValueError):self.c['probability_and_flag'](probabilities,[0,1])

    def test_artifact_metadata_and_fitted_stages_are_required(self):
        self.c['validate_fitted_pipeline'](self.pipeline,self.metadata)
        self.assertEqual(self.metadata['raw_features'],self.c['MODEL_FEATURES'])
        for key,value in [('feature_contract_hash','wrong'),('decision_operator','>='),('target_threshold_seconds',600),
                          ('raw_features',list(reversed(self.c['MODEL_FEATURES']))),('encoded_vector_size',475)]:
            m=copy.deepcopy(self.metadata);m[key]=value
            with self.assertRaises(ValueError):self.c['validate_artifact_metadata'](m)
        self.pipeline.stages=self.pipeline.stages[-1:]
        with self.assertRaisesRegex(ValueError,'complete fitted'):self.c['validate_fitted_pipeline'](self.pipeline,self.metadata)

    def test_load_uses_explicit_uri_metadata_then_native_pipeline_no_fit(self):
        calls=[]
        info=types.SimpleNamespace(metadata={self.c['ARTIFACT_METADATA_KEY']:self.metadata},run_id='new-fit-run')
        model,metadata=self.c['load_classifier_artifact']('runs:/new-fit-run/classifier_pipeline',
            lambda uri:(calls.append(('info',uri)) or info),lambda uri:(calls.append(('load',uri)) or self.pipeline))
        self.assertIs(model,self.pipeline);self.assertEqual(metadata,self.metadata)
        self.assertEqual([c[0] for c in calls],['info','load'])
        with self.assertRaisesRegex(ValueError,'MODEL_URI'):self.c['load_classifier_artifact'](None)
        wrong=types.SimpleNamespace(metadata={},run_id='new-fit-run')
        with self.assertRaisesRegex(ValueError,'lacks'):self.c['load_classifier_artifact']('runs:/x/model',lambda uri:wrong,lambda uri:self.fail('must not load'))
        info.run_id='historical-other-run'
        with self.assertRaisesRegex(ValueError,'origin'):self.c['load_classifier_artifact']('runs:/x/model',lambda uri:info,lambda uri:self.fail('must not load'))

    def test_persistence_requires_native_roundtrip_and_passes_storage_location(self):
        logged=[];loaded=[]
        fake=types.SimpleNamespace(active_run=lambda:types.SimpleNamespace(info=types.SimpleNamespace(run_id='new-fit-run')),
            spark=types.SimpleNamespace(log_model=lambda *args,**kwargs:(logged.append(kwargs) or types.SimpleNamespace(model_uri='runs:/new-fit-run/model'))))
        modules={'mlflow.models':types.SimpleNamespace(ModelSignature=lambda **kw:kw),
                 'mlflow.types.schema':types.SimpleNamespace(Schema=lambda v:v,ColSpec=lambda *a,**kw:a)}
        original=self.c['load_classifier_artifact']
        def loader(uri,**kw):
            loaded.append((uri,kw));return self.pipeline,self.metadata
        self.c['load_classifier_artifact']=loader
        try:
            with patch.dict(sys.modules,modules):
                uri,pipeline=self.c['persist_classifier_artifact'](self.pipeline,self.metadata,fake,
                    allow_persist=True,dfs_tmpdir='/Volumes/existing/schema/volume/tmp')
                self.assertIs(pipeline,self.pipeline)
                self.assertEqual(logged[0]['metadata'][self.c['ARTIFACT_METADATA_KEY']],self.metadata)
                self.assertEqual(loaded[0][1]['dfs_tmpdir'],logged[0]['dfs_tmpdir'])
                fake.spark.log_model=lambda *a,**kw:(_ for _ in ()).throw(RuntimeError('serialization unsupported'))
                with self.assertRaisesRegex(RuntimeError,'serialization unsupported'):
                    self.c['persist_classifier_artifact'](self.pipeline,self.metadata,fake,allow_persist=True)
        finally:self.c['load_classifier_artifact']=original
        info=types.SimpleNamespace(metadata={self.c['ARTIFACT_METADATA_KEY']:self.metadata},run_id='new-fit-run')
        seen=[]
        self.c['load_classifier_artifact']('runs:/new-fit-run/model',lambda uri:info,
            lambda uri,**kw:(seen.append(kw) or self.pipeline),dfs_tmpdir='/Volumes/existing/schema/volume/tmp')
        self.assertEqual(seen,[{'dfs_tmpdir':'/Volumes/existing/schema/volume/tmp'}])
        with self.assertRaisesRegex(ValueError,'pin a model version'):
            self.c['load_classifier_artifact']('models:/model@champion',lambda uri:info,lambda uri:self.pipeline)

    def test_preprocessing_builder_matches_frozen_configuration(self):
        names=['SQLTransformer','Imputer','StringIndexer','OneHotEncoder','VectorAssembler','StandardScaler']
        features=types.SimpleNamespace(**{name:type(name,(Stage,),{}) for name in names})
        ml=types.SimpleNamespace(Pipeline=lambda stages:types.SimpleNamespace(stages=stages))
        with patch.dict(sys.modules,{'pyspark.ml':ml,'pyspark.ml.feature':features}):
            stages=self.c['build_preprocessing_pipeline']().stages
        self.assertEqual([s.__class__.__name__ for s in stages],names)
        self.assertEqual(stages[1].params['strategy'],'median');self.assertEqual(stages[1].params['relativeError'],0.001)
        self.assertEqual(stages[2].params['stringOrderType'],'alphabetAsc')
        self.assertEqual(stages[2].params['handleInvalid'],'keep');self.assertEqual(stages[3].params['handleInvalid'],'keep')
        self.assertFalse(stages[3].params['dropLast']);self.assertEqual(stages[4].params['handleInvalid'],'error')
        self.assertTrue(stages[5].params['withStd']);self.assertFalse(stages[5].params['withMean'])
        self.assertEqual(len(stages[4].params['inputCols']),47)

    def test_preprocessing_matches_actual_historical_12_stage_parameters(self):
        from contextlib import redirect_stdout
        from io import StringIO
        names=['SQLTransformer','Imputer','StringIndexer','OneHotEncoder','VectorAssembler','StandardScaler']
        constructors={name:type(name,(Stage,),{}) for name in names}
        scope=dict(constructors,selected_numeric_features=self.c['NUMERICAL_FEATURES'],
            selected_categorical_features=self.c['CATEGORICAL_FEATURES'],feature_columns=self.c['MODEL_FEATURES'],
            forbidden_columns=self.c['MODEL_CONTRACT']['forbidden'])
        old=(ROOT/'notebooks/12_model_registration_and_scoring.py').read_text(encoding='utf-8')
        text=old[old.index('numeric_raw = ['):old.index('preprocessor_model = Pipeline(stages=stages).fit')]
        with redirect_stdout(StringIO()):exec(compile(text,'actual_historical_preprocessing','exec'),scope)
        with patch.dict(sys.modules,{'pyspark.ml':types.SimpleNamespace(Pipeline=lambda stages:types.SimpleNamespace(stages=stages)),
            'pyspark.ml.feature':types.SimpleNamespace(**constructors)}):
            new=self.c['build_preprocessing_pipeline']().stages
        self.assertEqual([s.__class__.__name__ for s in scope['stages']],[s.__class__.__name__ for s in new])
        self.assertEqual([s.params for s in scope['stages']],[s.params for s in new])

    def test_unknown_or_null_category_contract_is_enforced(self):
        for index in [2,3]:
            self.pipeline.stages[index].params['handleInvalid']='error'
            with self.assertRaisesRegex(ValueError,'handleInvalid'):self.c['validate_fitted_pipeline'](self.pipeline,self.metadata)
            self.pipeline.stages[index].params['handleInvalid']='keep'
        self.pipeline.stages[3].categorySizes=[3]*5
        with self.assertRaisesRegex(ValueError,'dimensions'):self.c['validate_fitted_pipeline'](self.pipeline,self.metadata)

    def test_actual_scoring_transform_preserves_identity_unseen_and_null_categories(self):
        from contextlib import ExitStack
        with ExitStack() as stack:
            for p in output_expressions():stack.enter_context(p)
            stack.enter_context(patch.dict(sys.modules,{'pyspark.ml.functions':types.SimpleNamespace(vector_to_array=col)}))
            for changes in [{},{'route_id':'unseen-route'},{'agency_id':'unseen-agency'},
                            {'current_status':'unseen-status'},{'route_id':None,'agency_id':None,'current_status':None}]:
                source=fixture()
                staging=self.c['prepare_scoring_dataset'](source,8,101,datetime.fromtimestamp(103,ROME),'batch')
                for row in staging.rows:row.update(changes)
                out=self.c['score_current_input'](staging,self.pipeline,self.metadata,datetime.fromtimestamp(104,ROME))
                self.assertEqual(out.count(),staging.count());self.assertEqual(out.columns,self.c['OUTPUT_COLUMNS'])
                for before,after in zip(staging.rows,out.rows):
                    self.assertEqual({k:before[k] for k in self.c['IDENTITY_COLUMNS']},{k:after[k] for k in self.c['IDENTITY_COLUMNS']})
                    self.assertEqual(after['major_delay_probability'],0.45);self.assertEqual(after['predicted_major_delay_flag'],0)
                    self.assertEqual(after['model_run_id'],'new-fit-run')
            for p,expected in [(0.45,0),(0.45001,1),(1.0,1),(0.0,0)]:
                self.pipeline.positive_probability=p
                out=self.c['score_current_input'](staging,self.pipeline,self.metadata,datetime.now())
                self.assertEqual(out.rows[0]['major_delay_probability'],p)
                self.assertEqual(out.rows[0]['predicted_major_delay_flag'],expected)
            staging.rows[0]['label_source_feed_timestamp']=999
            staging.types['label_source_feed_timestamp']='bigint'
            with self.assertRaisesRegex(ValueError,'Outcome'):self.c['score_current_input'](staging,self.pipeline,self.metadata,datetime.now())

    def test_no_inference_fit_labeled_read_or_writes_and_default_gates_off(self):
        scoring=(ROOT/'notebooks/12b_score_current_input.py').read_text(encoding='utf-8-sig')
        common=ast.parse((ROOT/'notebooks/12_common_persisted_classifier.py').read_text(encoding='utf-8-sig'))
        self.assertNotIn('next_stop_delay_labeled',scoring)
        functions=[n for n in common.body if isinstance(n,ast.FunctionDef) and n.name in
                   ['score_current_input','load_classifier_artifact','validate_scoring_input','validate_fitted_pipeline']]
        for tree in [ast.parse(scoring),ast.Module(body=functions,type_ignores=[])]:
            forbidden={'fit','saveAsTable','save','register_model','log_model','start_run'}
            self.assertFalse(any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr in forbidden for n in ast.walk(tree)))
        for name,gates in [('12a_fit_persist_final_classifier.py',['ALLOW_MODEL_FIT','ALLOW_MODEL_PERSIST','ALLOW_UC_REGISTRATION']),
                           ('12b_score_current_input.py',['ALLOW_MODEL_INFERENCE','ALLOW_SCORING_WRITE'])]:
            tree=ast.parse((ROOT/'notebooks'/name).read_text(encoding='utf-8-sig'))
            values={t.id:ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.value,ast.Constant) for t in n.targets if isinstance(t,ast.Name)}
            for gate in gates:self.assertIs(values[gate],False)
        with self.assertRaisesRegex(RuntimeError,'disabled'):
            self.c['persist_classifier_artifact'](self.pipeline,self.metadata,None)


if __name__=='__main__':unittest.main()
