# Hw02
本次作业为在TuGraph上测试医疗对话助手  
## 数据处理  
首先我们还是要服从图数据库的基本流程，构建关系词和实体，仍然是从节点到边，最后到图的完整的流程
```python
import pandas as pd
import json
import os

COLS = ['name', 'alias', 'part', 'age', 'infection', 'insurance', 'department', 'checklist', 'symptom', 'complication', 'treatment', 'drug', 'period', 'rate', 'money']

NODES = {
    'symptom': 'Symptom',
    'department': 'Department', 
    'drug': 'Drug',
    'complication': 'Complication',
    'checklist': 'CheckItem'
}

EDGE_MAPPING = {
    'symptom': 'HAS_SYMPTOM',
    'department': 'IS_OF_Department',
    'drug': 'HAS_Drug',
    'complication': 'HAS_Complication',
    'checklist': 'HAS_Checklist'
}

def load_and_split(csv_path='data.csv'):
    df = pd.read_csv(csv_path)
    print(df.columns.tolist())
    df = df[COLS] 

    disease_df = df[['name', 'alias', 'part', 'age', 'infection', 'insurance', 'treatment', 'period', 'rate', 'money']].copy()
    
    entities = {'Disease': disease_df}

    for col, label in NODES.items(): 
        unique_values = set()
        for val in df[col].dropna(): 
            for v in str(val).split(','):
                v = v.strip()
                if v : 
                    unique_values.add(v) 
        entities[label] = pd.DataFrame({"name": sorted(unique_values)})

    edges = []
    for col, edge_label in EDGE_MAPPING.items():
        for _, row in df.iterrows():
            disease_name = row['name']
            if pd.notna(row[col]):
                values = str(row[col]).split(',')
                for val in values:
                    val = val.strip()
                    if val:
                        edges.append({
                            "src": disease_name,
                            "dst": val,
                            "label": edge_label
                        })

    return entities, pd.DataFrame(edges)

def generate_schema():
    return {
        "schema": [
            {
                "label": "Disease",
                "type": "VERTEX",
                "properties": [
                    {"name": "name", "type": "STRING", "optional": False, "index": True},
                    {"name": "alias", "type": "STRING", "optional": True},
                    {"name": "part", "type": "STRING", "optional": True},
                    {"name": "age", "type": "STRING", "optional": True},
                    {"name": "infection", "type": "STRING", "optional": True},
                    {"name": "insurance", "type": "STRING", "optional": True},
                    {"name": "treatment", "type": "STRING", "optional": True},
                    {"name": "period", "type": "STRING", "optional": True},
                    {"name": "rate", "type": "STRING", "optional": True},
                    {"name": "money", "type": "STRING", "optional": True}
                ],
                "primary": "name"
            },
            {
                "label": "Symptom",
                "type": "VERTEX",
                "properties": [
                    {"name": "name", "type": "STRING", "optional": False, "unique": True, "index": True}
                ],
                "primary": "name"
            },
            {
                "label": "Department",
                "type": "VERTEX",
                "properties": [
                    {"name": "name", "type": "STRING", "optional": False, "unique": True, "index": True}
                ],
                "primary": "name"
            },
            {
                "label": "Drug",
                "type": "VERTEX",
                "properties": [
                    {"name": "name", "type": "STRING", "optional": False, "unique": True, "index": True}
                ],
                "primary": "name"
            },
            {
                "label": "Complication",
                "type": "VERTEX",
                "properties": [
                    {"name": "name", "type": "STRING", "optional": False, "unique": True, "index": True}
                ],
                "primary": "name"
            },
            {
                "label": "CheckItem",
                "type": "VERTEX",
                "properties": [
                    {"name": "name", "type": "STRING", "optional": False, "unique": True, "index": True}
                ],
                "primary": "name"
            },
            {
                "label": "HAS_SYMPTOM",
                "type": "EDGE",
                "properties": [],
                "constraints": [["Disease", "Symptom"]]
            },
            {
                "label": "IS_OF_Department",
                "type": "EDGE",
                "properties": [],
                "constraints": [["Disease", "Department"]]
            },
            {
                "label": "HAS_Drug",
                "type": "EDGE",
                "properties": [],
                "constraints": [["Disease", "Drug"]]
            },
            {
                "label": "HAS_Complication",
                "type": "EDGE",
                "properties": [],
                "constraints": [["Disease", "Complication"]]
            },
            {
                "label": "HAS_Checklist",
                "type": "EDGE",
                "properties": [],
                "constraints": [["Disease", "CheckItem"]]
            }
        ]
    }

def main():
    entities, edges_df = load_and_split()
    
    os.makedirs("Nodes_Edges", exist_ok=True)
    os.makedirs("Graph", exist_ok=True)
    
    for node_label, df_node in entities.items():
        df_node.to_csv(f"Nodes_Edges/{node_label}.csv", index = False)
        print(f"Saved Node:{node_label} ({len(df_node)} of record)")
    
    for edge_label in EDGE_MAPPING.values():
        edge_subset = edges_df[edges_df['label'] == edge_label][['src', 'dst']]
        if not edge_subset.empty:
            target_label = [k for k, v in EDGE_MAPPING.items() if v == edge_label][0]
            target_node = NODES[target_label]

            edge_subset.columns = ["Disease:name", f"{target_node}:name"]
            edge_subset.to_csv(f"Nodes_Edges/{edge_label}.csv", index = False)
            print(f"Saved Edge: {edge_label} ({len(edge_subset)} of record")
        
    with open("Graph/schema.json", "w", encoding="utf-8") as f:
        json.dump(generate_schema(), f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    main()
```
上面的代码即用于构建csv数据中读取实体，以及实体之间的联系，实体的属性，之后把他们到处成为独立的数据，构成节点和边  
那么既然导入成功，之后我们就需要把他上传到对应的TuGraph的数据库上  
## 导入TuGraph数据库

```python
import os
import json
import subprocess

def get_tugraph_container():
    result = subprocess.run(
        ["docker", "ps", "--filter", "ancestor=tugraph/tugraph-runtime-centos7:3.4.0", "--format", "{{.Names}}"],
        capture_output=True,
        text=True
    )
    containers = result.stdout.strip().split('\n')
    if containers and containers[0]:
        return containers[0]
    else:
        print("Error: Cannot find TuGraph container")
        print("Set TuGraph container up first, or point the container name")
        return None

def generate_import_config():

    with open("./Graph/schema.json", "r", encoding="utf-8") as f:
        schema_data = json.load(f)

    config = {
        "schema": schema_data["schema"],

        "files": [
            {"path": "/data/Nodes_Edges/Disease.csv", "format": "CSV", "label": "Disease", "header": 1,
             "columns": ["name", "alias", "part", "age", "infection", "insurance", "treatment", "period", "rate", "money"]},
            {"path": "/data/Nodes_Edges/Symptom.csv", "format": "CSV", "label": "Symptom", "header": 1, "columns": ["name"]},
            {"path": "/data/Nodes_Edges/Department.csv", "format": "CSV", "label": "Department", "header": 1, "columns": ["name"]},
            {"path": "/data/Nodes_Edges/Drug.csv", "format": "CSV", "label": "Drug", "header": 1, "columns": ["name"]},
            {"path": "/data/Nodes_Edges/Complication.csv", "format": "CSV", "label": "Complication", "header": 1, "columns": ["name"]},
            {"path": "/data/Nodes_Edges/CheckItem.csv", "format": "CSV", "label": "CheckItem", "header": 1, "columns": ["name"]},
            
            {"path": "/data/Nodes_Edges/HAS_SYMPTOM.csv", "format": "CSV", "label": "HAS_SYMPTOM", "header": 1,
             "SRC_ID": "Disease", "DST_ID": "Symptom", "columns": ["SRC_ID", "DST_ID"]},
            {"path": "/data/Nodes_Edges/IS_OF_Department.csv", "format": "CSV", "label": "IS_OF_Department", "header": 1,
             "SRC_ID": "Disease", "DST_ID": "Department", "columns": ["SRC_ID", "DST_ID"]},
            {"path": "/data/Nodes_Edges/HAS_Drug.csv", "format": "CSV", "label": "HAS_Drug", "header": 1,
             "SRC_ID": "Disease", "DST_ID": "Drug", "columns": ["SRC_ID", "DST_ID"]},
            {"path": "/data/Nodes_Edges/HAS_Complication.csv", "format": "CSV", "label": "HAS_Complication", "header": 1,
             "SRC_ID": "Disease", "DST_ID": "Complication", "columns": ["SRC_ID", "DST_ID"]},
            {"path": "/data/Nodes_Edges/HAS_Checklist.csv", "format": "CSV", "label": "HAS_Checklist", "header": 1,
             "SRC_ID": "Disease", "DST_ID": "CheckItem", "columns": ["SRC_ID", "DST_ID"]}
        ]
    }
    
    os.makedirs("Graph", exist_ok=True)
    with open("Graph/import.conf", "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    print("Configuration finished")

def import_data(container_name=None, graph_name="medical_graph"):
    
    if container_name is None:
        container_name = get_tugraph_container()
        if container_name is None:
            return
    
    print(f"Using Container: {container_name}")
    
    generate_import_config()
    
    try:
        subprocess.run(["docker", "cp", "./Nodes_Edges", f"{container_name}:/data/"], check=True)
        subprocess.run(["docker", "cp", "./Graph/import.conf", f"{container_name}:/data/"], check=True)
        print("Successfully copy data")
    except subprocess.CalledProcessError as e:
        print(f"Copy Failure: {e}")
        return
    
    import_cmd = [
        "docker", "exec", container_name,
        "lgraph_import",
        "-c", "/data/import.conf",
        "-d", "/var/lib/lgraph/data",
        "--graph", graph_name,
        "-i", "true",
        "--overwrite", "true",
        "--verbose", "2", 
        "--user", "admin",
        "--password", "Szh168kk"
    ]
    
    result = subprocess.run(import_cmd, capture_output=True, text=True)
    
    print(result.stdout)
    
    if result.returncode == 0:
        print("\nSuccessfully import data")
    else:
        print("\nImport Failure")
        print(result.stderr)

if __name__ == "__main__": 
    import_data(container_name="XXXX", graph_name="XXXX")
```
之后我们找到对应的端口映射即可，需要等待数据上传一段时间，那么我们可以查下关系图，用Cypher语句在TuGraph的图形化界面

```Cypher
MATCH (n)-[r]->(m)
RETURN n, r, m
LIMIT 40
```
![CYPHER](./pic/Cypher.png)


