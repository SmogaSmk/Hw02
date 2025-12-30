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
