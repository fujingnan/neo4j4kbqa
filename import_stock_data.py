from py2neo import Node, Subgraph, Graph, Relationship, NodeMatcher
from tqdm import tqdm
import pandas as pd

graph = Graph("bolt://localhost:11005", auth=("fujingnan", "123"))

def import_stock():
    df = pd.read_csv('股票.csv')
    sid = df['sid'].values
    sname = df['sname'].values

    nodes = []
    data = list(zip(sid, sname))
    for sid, sname in tqdm(data):
        node = Node('股票', sname=sname, symbol=str(sid))
        nodes.append(node)

    graph.create(Subgraph(nodes))

def import_block():
    df = pd.read_csv('板块.csv')
    bid = df['bid'].values
    bname = df['bname'].values

    nodes = []
    data = list(zip(bid, bname))
    for bid, bname in tqdm(data):
        node = Node('板块', bname=bname, block_id=str(bid))
        nodes.append(node)

    graph.create(Subgraph(nodes))

def import_word():
    df = pd.read_csv('关键词.csv')
    wid = df['wid'].values
    wname = df['wname'].values

    nodes = []
    data = list(zip(wid, wname))
    for wid, wname in tqdm(data):
        node = Node('行业词', wname=wname, word_id=str(wid))
        nodes.append(node)

    graph.create(Subgraph(nodes))

def import_relation():
    df = pd.read_csv('板块-股票.csv')
    matcher = NodeMatcher(graph)
    bid = df['bid'].values
    sid = df['sid'].values
    relations = []
    data = list(zip(bid, sid))
    for b, s in tqdm(data):
        block = matcher.match('板块', block_id=str(b)).first()
        stock = matcher.match('股票', symbol=str(s)).first()
        if block is not None and stock is not None:
            relations.append(Relationship(block, '包含', stock))
        else:
            print('None')

    graph.create(Subgraph(relationships=relations))
    print('import block-stock relation succeeded')

    df = pd.read_csv('关键词-股票.csv')
    matcher = NodeMatcher(graph)
    wid = df['wid'].values
    sid = df['sid'].values
    relations = []
    data = list(zip(wid, sid))
    for w, s in tqdm(data):
        word = matcher.match('行业词', word_id=str(w)).first()
        stock = matcher.match('股票', symbol=str(s)).first()
        if word is not None and stock is not None:
            relations.append(Relationship(word, '关联股票', stock))
        else:
            print('None')

    graph.create(Subgraph(relationships=relations))
    print('import word-stock relation succeeded')

    df = pd.read_csv('关键词-板块.csv')
    matcher = NodeMatcher(graph)
    wid = df['wid'].values
    bid = df['bid'].values
    relations = []
    data = list(zip(wid, bid))
    for w, b in tqdm(data):
        word = matcher.match('行业词', word_id=str(w)).first()
        block = matcher.match('板块', block_id=str(b)).first()
        if word is not None and block is not None:
            relations.append(Relationship(word, '关联板块', block))
        else:
            print('None')

    graph.create(Subgraph(relationships=relations))
    print('import word-block relation succeeded')

def delete_relation():
    cypher = 'match ()-[r]-() delete r'
    graph.run(cypher)

# def delete_property():
#     cypher = 'match ()'

def delete_node():
    cypher = 'match (n) delete n'
    graph.run(cypher)

def import_data():
    import_stock()
    import_block()
    import_word()
    import_relation()


def delete_data():
    delete_relation()
    delete_node()
    print('delete data succeeded')

if __name__ == '__main__':
    delete_data()
    import_data()