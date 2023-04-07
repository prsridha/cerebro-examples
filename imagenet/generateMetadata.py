import os
import json
import xmltodict
import pandas as pd
from pathlib import Path

def generate_label_mapping(train_path, mapping_path):
    idx = 0
    mapping = {}
    for dir in Path(train_path).iterdir():
        label = dir.name.split("/")[-1]
        mapping[label] = idx
        idx += 1
    with open(mapping_path, "w") as f:
        json.dump(mapping, f, indent=2)

def read_datatree_files(path):
    with open(path) as f:
        txt = f.read()
        for row in txt.split("\n"):
            if row != "":
                res = row.split()[3]
                yield res

def read_annotation_files(path):
    for xml_path in Path(path).rglob("*.xml"):
        with open(xml_path) as f:
            xmls = f.read()
            xml = xmltodict.parse(xmls)
            yield xml["annotation"]

def generate_train_metadata(from_path, to_path, mapping_path):
    count = 1
    dataset = {
        'filename': [],
        'filepath': [],
        'original_label': [],
        'label': []
    }

    with open(mapping_path) as f:
        mapping = json.load(f)
    
    for line in read_datatree_files(from_path):
        row = {}

        row["filename"] = os.path.basename(line)
        row["filepath"] = line

        row["original_label"] = line.split("/")[3]
        row["label"] = mapping[row["original_label"]]

        for i in row:
            dataset[i].append(row[i])

        print(count)
        count += 1
    
    pd_df = pd.DataFrame(dataset)
    pd_df.index.name = "id"
    pd_df.to_csv(to_path)

def generate_val_metadata(from_path, to_path, mapping_path):
    count = 1
    root_obj_path = "Data/CLS-LOC/valid"
    dataset = {
        # 'width': [],
        # 'height': [],
        # 'depth': [],
        'filename': [],
        'filepath': [],
        'original_label': [],
        'label': []
    }

    with open(mapping_path) as f:
        mapping = json.load(f)
    
    for xml in read_annotation_files(from_path):
        row = {}

        # row["width"] = xml["size"]["width"]
        # row["height"] = xml["size"]["height"]
        # row["depth"] = xml["size"]["depth"]
        row["filename"] = xml["filename"] + ".JPEG"

        row["filepath"] = os.path.join(root_obj_path, row["filename"])

        if type(xml["object"]) == type(list()):
            xml["object"] = xml["object"][0]

        row["original_label"] = xml["object"]["name"]
        row["label"] = mapping[xml["object"]["name"]]

        for i in row:
            dataset[i].append(row[i])

        print(count)
        count += 1
    
    pd_df = pd.DataFrame(dataset)
    pd_df.index.name = "id"
    pd_df.to_csv(to_path)

def main():
    train_path = "/home/ec2-user/imagenet/DataTree/train.txt"
    train_meta_path = "/home/ec2-user/imagenet/Metadata/train.csv"
    
    val_path = "/home/ec2-user/imagenet/Annotations/CLS-LOC/val"
    val_meta_path = "/home/ec2-user/imagenet/Metadata/valid.csv"

    mapping_path = "/home/ec2-user/imagenet/Metadata/imagenet_label_mapping.json"

    # generate_label_mapping(train_path, mapping_path)
    generate_train_metadata(train_path, train_meta_path, mapping_path)
    # generate_val_metadata(val_path, val_meta_path, mapping_path)

main()