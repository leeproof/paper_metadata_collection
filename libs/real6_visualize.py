import pandas as pd
import ast
import networkx as nx
from pyvis.network import Network
from collections import defaultdict
import itertools
import os

# 1. org_names 컬럼을 실제 문자열 리스트로 파싱 + 비문자열 요소 제거
def parse_and_clean(cell):
    """
    - cell: "['Inst A', None, 'Inst B']" 또는 ['Inst A', None, 'Inst B'] 형태
    - 문자열만 남긴 리스트 반환
    """
    # (1) 문자열이면 literal_eval 시도
    if isinstance(cell, str):
        try:
            lst = ast.literal_eval(cell)
        except (ValueError, SyntaxError):
            lst = []
    # (2) 이미 list/tuple 이면 그대로
    elif isinstance(cell, (list, tuple)):
        lst = cell
    else:
        lst = []
    # (3) 문자열이 아닌 요소 모두 필터링
    return [x for x in lst if isinstance(x, str) and x.strip()]

def visualize_network(graph, html_path, scale=5, base_size=10):
    net = Network(
        height="750px", width="%100",
        notebook=False, bgcolor="#1e1e1e", font_color="white",
        directed=False
    )
    # 물리엔진 설정은 기본값 사용
    net.show_buttons(filter_=["physics"])
    for node, data in graph.nodes(data=True):
        size = base_size + scale * data.get('weight', 0)
        net.add_node(
            node,
            label=node,
            title=f"빈도: {data.get('weight', 0)}",
            value=size
        )
    for u, v, data in graph.edges(data=True):
        net.add_edge(
            u, v,
            value=data.get('weight', 1),
            title=f"동시출현: {data.get('weight', 0)}회"
        )
    net.write_html(html_path)
    print(f"시각화 저장 완료 → {html_path}")

def main(input_csv: str, output_html: str | None = None, scale: int = 5, base_size: int = 10):
    # 1) 입력 CSV 로드 (real5 출력: or_names 필요)
    df = pd.read_csv(input_csv, encoding='utf-8')
    if 'org_names' not in df.columns:
        raise ValueError("real6는 real5의 출력 CSV를 입력으로 합니다. 'org_names' 컬럼이 없습니다.")
    
    # 2) org_names 파싱 및 정리
    df['inst_list'] = df['org_names'].apply(parse_and_clean)

    # 3) 노드/엣지 가중치 계산
    node_weight = defaultdict(int)
    edge_weight = defaultdict(int)
    for insts in df['inst_list']:
        unique_insts = set(insts)
        # 노드 가중치: 한 행에 등장한 기관 수만큼 카운트
        for inst in unique_insts:
            node_weight[inst] += 1
        # 엣지 가중치: 같은 행에 함께 등장한 기관 쌍
        for a, b in itertools.combinations(sorted(unique_insts), 2):
            edge_weight[(a, b)] += 1

    # 4) NetworkX 그래프 생성
    G = nx.Graph()
    for inst, w in node_weight.items():
        G.add_node(inst, weight=w)
    for (a, b), w in edge_weight.items():
        G.add_edge(a, b, weight=w)

    # 5) HTML 파일 경로 설정
    html_path = output_html or (os.path.splitext(input_csv)[0] + "_network.html")
    print(f"노드 수: {len(node_weight):,} | 엣지 수: {len(edge_weight):,}")

    # 6) 네트워크 시각화
    visualize_network(G, html_path=html_path, scale=scale, base_size=base_size)
    return html_path

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("input_csv")
    p.add_argument("--output_html", default=None)
    p.add_argument("--scale", type=int, default=5)
    p.add_argument("--base_size", type=int, default=10)
    args = p.parse_args()
    main(args.input_csv, output_html=args.output_html, scale=args.scale, base_size=args.base_size)