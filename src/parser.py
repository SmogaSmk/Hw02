from neo4j import GraphDatabase
import re
import logging

MOTIVATIONS_CONFIG = {
    'symptom_to_disease': {'keywords': {'症状', '病情', '表现', '特征', '可能是什么病', '什么病', '为什么', '原因', '怎么', '怎么造成'}},
    'disease_symptoms': {'keywords': {'症状', '情况', '特征', '有哪些症状', '症状有哪些', '什么症状'}},
    'disease_drugs': {'keywords': {'服用', '服药', '药', '用药', '吃什么药', '用什么药', '药物', '啥药'}},
    'disease_department': {'keywords': {'科室', '挂什么科', '看什么科', '什么科', '什么科室', '部门', '什么部门'}},
    'disease_treatment': {'keywords': {'治疗', '怎么治', '如何治', '治疗方法', '治疗方案', '怎么办'}},
    'disease_complications': {'keywords': {'并发症', '并发疾病'}},
    'disease_info': {'keywords': {'信息', '详情', '介绍', '是什么', '什么是', '有什么', '有哪些'}},
}

CYPHER_BOARDS = {
    'symptom_to_disease': """
        MATCH (s:Symptom)-[:HAS_SYMPTOM]-(d:Disease)
        WHERE s.name IN [{symptoms}]
        RETURN d.name AS disease, collect(s.name) AS matched_symptoms LIMIT 10
    """,
    'disease_symptoms': """
        MATCH (d:Disease {{name: "{disease}"}})-[:HAS_SYMPTOM]->(s:Symptom)
        RETURN d.name AS disease, collect(s.name) AS symptoms
    """,
    'disease_drugs':"""
        MATCH (d:Disease {{name: "{disease}"}})-[:HAS_Drug]->(dr:Drug)
        RETURN d.name AS disease, collect(dr.name) AS drugs
    """,
    'disease_department':"""
        MATCH (d:Disease {{name: "{disease}"}})-[:IS_OF_Department]->(dp:Department)
        RETURN d.name AS disease, collect(dp.name) AS departments
    """,
    'disease_treatment':"""
        MATCH (d:Disease {name: "{disease}"})
        RETURN d.name AS disease, d.treatment AS treatment, d.period AS period
    """
}

class EntityLoader:
    def __init__(self, driver, dbs):
        self.driver = driver
        self.dbs = dbs 
        self.diseases = [] 
        self.symptoms = [] 
        self.load()

    def _load(self, label): 
        cypher = f"MATCH (n:{label}) RETURN n.name AS name" 
        with self.driver.session(database=self.dbs) as session: 
            result = session.run(cypher)
            return [r["name"].lower() for r in result if r["name"]]

    def load(self):
        self.diseases = self._load("Disease")
        self.symptoms = self._load("Symptom")

class MotivationDetector: 
    @staticmethod
    def detect(text: str) -> str:
        text = text.lower()
        best, max_score = 'disease_info', 0
        for motivation, data in MOTIVATIONS_CONFIG.items():
            score = sum(k in text for k in data['keywords'])
            if score > max_score:
                max_score, best = score, motivation
        return best

class EntityExtractor: 
    def __init__(self, entities): 
        self.entities = entities

    def extract(self, text): 
        text = text.lower()
        diseases = [d for d in self.entities.diseases if d in text]
        symptoms = [s for s in self.entities.symptoms if s in text]
        if not diseases and not symptoms:
            stopwords = {'怎么', '什么', '哪些', '为什么', '吗', '呢', '了', '的', '是'}
            words = [w for w in text.split() if len(w) > 1 and w not in stopwords]
            symptoms = words if words else [text]
        return {'diseases': diseases, 'symptoms': symptoms}

class CypherCreator:
    @staticmethod
    def escape(value):
        return value.replace("'", "\\'")
    @staticmethod
    def generate(motivation, entities):
        if motivation == 'symptom_to_disease':
            symptoms = entities.get('symptoms', [])
            if not symptoms:
                return None
            symptom_str = ', '.join(f'"{s}"' for s in symptoms)
            return CYPHER_BOARDS['symptom_to_disease'].format(symptoms=symptom_str)

        if not entities['diseases']:
            return None

        disease = CypherCreator.escape(entities['diseases'][0])
        template = CYPHER_BOARDS.get(motivation)
        if not template:
            return None

        return template.format(disease=disease)

class QueryExecutor:
    def __init__(self, driver, dbs):
        self.driver = driver
        self.dbs = dbs

    def execute(self, cypher: str):
        if not cypher:
            return [{"error": "Cannot execute Retrieval"}]
        try:
            with self.driver.session(database=self.dbs) as session:
                result = session.run(cypher)
                return [dict(r) for r in result]
        except Exception as e:
            return [{"error": str(e)}]


class QueryParserAnalyser:
    def __init__(self, uri, usr, passwd, dbs): 
        self.driver = GraphDatabase.driver(uri, auth = (usr, passwd))
        self.dbs = dbs
        self.entities = EntityLoader(self.driver, dbs)
        self.motivation_detect = MotivationDetector()
        self.entity_extractor = EntityExtractor(self.entities)
        self.cypher_gen = CypherCreator()
        self.executor = QueryExecutor(self.driver, dbs)

    def parser_query(self, user_input):
        text = re.sub(r'[^\w\s]', '', user_input.lower()).strip()
        motivation = self.motivation_detect.detect(text)
        entities = self.entity_extractor.extract(text)
        cypher = self.cypher_gen.generate(motivation, entities)
        result = self.executor.execute(cypher)
        return {
            'original_input': user_input,
            'motivation': motivation,
            'entities': entities,
            'cypher': cypher,
            'result': result
        }

    def format_result(self, result):

        output = []
        output.append(f"Question: {result['original_input']}")
        output.append(f"motivation: {result['motivation']}")
        output.append(f"entities: {result['entities']}")
        output.append(f"Cypher: {result['cypher']}")
        output.append("Result:")

        for item in result['result']:
            if isinstance(item, dict) and 'error' in item:
                output.append(f"  Error: {item['error']}")
            else:
                output.append("  " + str(item))

        return "\n".join(output)
    
if __name__ == "__main__":
    URI = "bolt://localhost:7690"
    usr = "admin"
    passwd = "Szh168kk"
    dbs = "MedicalGraph"

    parser = QueryParserAnalyser(uri = URI, usr = usr, passwd = passwd, dbs = dbs)

    test_queries = [
        "头痛，可能是什么原因？",
        "癫痫的症状有哪些？",
        "糖尿病吃什么药？",
        "心脏病挂什么科？",
        "肺炎的基本信息", 
        "尿毒症吃什么药",
        "肾虚怎么治疗"
        
    ]

    try:
        for query in test_queries:
            print("<=>" * 30)
            result = parser.parser_query(query)
            formatted_output = parser.format_result(result)
            print(formatted_output)
            print("<=>" * 50)
            print()
    except Exception as e:
        import traceback
        print(f"错误: {e}")
        traceback.print_exc()
        print(f"错误: {e}")
    print("-" * 50)


