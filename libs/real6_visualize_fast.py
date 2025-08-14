# real6_visualize_fast.py
import pandas as pd
import ast, itertools, os
import networkx as nx
from collections import defaultdict
from statistics import quantiles
from pyvis.network import Network

def parse_and_clean(cell):
    if isinstance(cell, str):
        try:
            lst = ast.literal_eval(cell)
        except Exception:
            lst = []
    elif isinstance(cell, (list, tuple)):
        lst = cell
    else:
        lst = []
    return [x for x in lst if isinstance(x, str) and x.strip()]

def build_graph_from_csv(input_csv):
    df = pd.read_csv(input_csv, encoding="utf-8")
    if "org_names" not in df.columns:
        raise ValueError("'org_names' 컬럼이 없습니다.")
    df["inst_list"] = df["org_names"].apply(parse_and_clean)

    node_w = defaultdict(int)
    edge_w = defaultdict(int)
    for insts in df["inst_list"]:
        s = set(insts)
        for a in s:
            node_w[a] += 1
        for a, b in itertools.combinations(sorted(s), 2):
            edge_w[(a, b)] += 1

    G = nx.Graph()
    for n, w in node_w.items():
        G.add_node(n, weight=w)
    for (a, b), w in edge_w.items():
        G.add_edge(a, b, weight=w)
    return G

def k_core_filter(G, k=2):
    if G.number_of_nodes() == 0:
        return G
    try:
        H = nx.k_core(G, k=k)
    except nx.NetworkXError:
        H = G.copy()
    return H

def largest_cc(G):
    if G.number_of_nodes() == 0:
        return G
    cc = max(nx.connected_components(G), key=len)
    return G.subgraph(cc).copy()

def keep_topk_edges_per_node(G, top_k=8):
    H = nx.Graph()
    H.add_nodes_from(G.nodes(data=True))
    for n in G.nodes():
        nbrs = [(n, v, d.get("weight", 1)) for v, d in G[n].items()]
        nbrs.sort(key=lambda x: x[2], reverse=True)
        for u, v, w in nbrs[:top_k]:
            if H.has_edge(u, v):
                continue
            H.add_edge(u, v, **{"weight": w})
    return H

def filter_by_min_edge(G, min_edge=2, use_quantile=False, q=0.75):
    if G.number_of_edges() == 0:
        return G
    if use_quantile:
        weights = [d.get("weight", 1) for _,_,d in G.edges(data=True)]
        # 0~1 사이 q-분위
        thr = sorted(weights)[int(len(weights)*q)-1] if len(weights)>1 else weights[0]
        min_edge = max(min_edge, thr)
    H = nx.Graph()
    for u,v,d in G.edges(data=True):
        if d.get("weight",1) >= min_edge:
            H.add_edge(u,v,**d)
    for n,d in G.nodes(data=True):
        if n in H:
            H.nodes[n].update(d)
    return H

def precompute_layout(G, layout="spring", seed=42):
    if layout == "spring":
        pos = nx.spring_layout(G, seed=seed, k=None, iterations=200, dim=2)
    elif layout == "fr":
        pos = nx.fruchterman_reingold_layout(G, seed=seed, dim=2, iterations=1000)
    else:
        pos = nx.kamada_kawai_layout(G, dim=2)
    nx.set_node_attributes(G, {n: {"x": float(x), "y": float(y)} for n,(x,y) in pos.items()})
    return G

def to_pyvis(G, html_path, scale=4, base_size=6, physics=False):
    net = Network(height="800px", width="100%", notebook=False,
                  bgcolor="#111", font_color="#eaeaea", directed=False)

    # 성능을 위한 옵션
    net.set_options("""
    const options = {
      nodes: {
        shape: "dot",
        font: { size: 0 },            // 기본 라벨 비표시 (줌/호버시만)
        scaling: { min: 2, max: 40 }
      },
      edges: {
        smooth: false,
        color: { opacity: 0.5 },
        selectionWidth: 1,
        hoverWidth: 0
      },
      interaction: {
        hover: true,
        tooltipDelay: 50,
        hideEdgesOnDrag: true,
        hideEdgesOnZoom: true,
        zoomView: true
      },
      physics: {
        enabled: %s,
        stabilization: { iterations: 100, updateInterval: 25 }
      }
    }""" % ( "true" if physics else "false")
    )

    for n, d in G.nodes(data=True):
        size = base_size + scale * float(d.get("weight", 1)) ** 0.5
        net.add_node(n,
            label=n,  # label은 두되 font size 0 → 호버 시 title로 노출
            title=f"{n} | freq={d.get('weight',0)}",
            value=size,
            x=d.get("x"), y=d.get("y"), fixed=True
        )
    for u,v,d in G.edges(data=True):
        net.add_edge(u, v, value=float(d.get("weight",1)),
                     title=f"co-occurrence={d.get('weight',1)}")
    net.write_html(html_path)
    return html_path

def main(input_csv, output_html=None,
         min_edge=2, use_quantile=True, q=0.75,
         kcore=2, lcc_only=True, top_k=8,
         layout="spring", physics=False):
    G = build_graph_from_csv(input_csv)

    # 1) 약한 엣지 컷
    G = filter_by_min_edge(G, min_edge=min_edge, use_quantile=use_quantile, q=q)
    # 2) k-core
    if kcore and kcore > 1:
        G = k_core_filter(G, k=kcore)
    # 3) LCC만
    if lcc_only and G.number_of_nodes() > 0:
        G = largest_cc(G)
    # 4) 노드별 상위 엣지만 유지
    if top_k and top_k > 0:
        G = keep_topk_edges_per_node(G, top_k=top_k)
    # 5) 좌표 사전 계산 & 고정
    if G.number_of_nodes() > 0:
        G = precompute_layout(G, layout=layout, seed=42)

    html_path = output_html or (os.path.splitext(input_csv)[0] + "_network_fast.html")
    return to_pyvis(G, html_path, scale=4, base_size=6, physics=physics)

if __name__ == "__main__":
    import argparse, os
    p = argparse.ArgumentParser()
    p.add_argument("input_csv")
    p.add_argument("--output_html", default=None)
    p.add_argument("--min_edge", type=int, default=2)
    p.add_argument("--use_quantile", action="store_true")
    p.add_argument("--q", type=float, default=0.75)
    p.add_argument("--kcore", type=int, default=2)
    p.add_argument("--lcc_only", action="store_true")
    p.add_argument("--top_k", type=int, default=8)
    p.add_argument("--layout", choices=["spring","fr","kk"], default="spring")
    p.add_argument("--physics", action="store_true")
    args = p.parse_args()
    main(**vars(args))
