# -*- coding: utf-8 -*-
"""
real6_visualize_fast.py
빠른 협력 네트워크 시각화를 위한 안전/안정 버전

개선 사항
- Pyvis set_options(JSON) 사용 시 JSONDecodeError 수정: dict -> json.dumps 로 전달
- CSV 파싱 견고화: ast.literal_eval 기반 리스트 파싱, NaN/빈값 처리
- 대용량 방어: 자동 임계값(min_edge='auto', quantile 기반), top_k 상한, LCC/k-core 필터
- 그래프 없음/엣지 없음 상황에서도 HTML 정상 생성
"""
from __future__ import annotations

import os
import ast
import json
import math
from pathlib import Path
from typing import Iterable, List, Tuple, Dict, Any, Optional, Set

import pandas as pd
import networkx as nx
from pyvis.network import Network


# -----------------------------
# 유틸
# -----------------------------
def _safe_literal_list(x: Any) -> List[Any]:
    """문자열/리스트/NaN을 안전하게 파싱해서 list 로 반환."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s == "":
            return []
        # 리스트 형태로 보이는 경우만 literal_eval
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
            try:
                v = ast.literal_eval(s)
                if isinstance(v, (list, tuple)):
                    return list(v)
                return [v]
            except Exception:
                # 콤마로 분리 시도
                try:
                    return [t.strip() for t in s.split(",") if t.strip()]
                except Exception:
                    return [s]
        # JSON 형태?
        if s.startswith("{") and s.endswith("}"):
            try:
                v = json.loads(s)
                if isinstance(v, list):
                    return v
                return [v]
            except Exception:
                return [s]
        # 그 외에는 콤마/세미콜론 분리
        for sep in [";", "|", ","]:
            if sep in s:
                parts = [t.strip() for t in s.split(sep) if t.strip()]
                if parts:
                    return parts
        return [s]
    # 기타 타입
    return [x]


def _unique_preserve(seq: Iterable[Any]) -> List[Any]:
    """순서를 유지하며 중복 제거."""
    seen: Set[Any] = set()
    out: List[Any] = []
    for v in seq:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


# -----------------------------
# 데이터 -> 엣지 집계
# -----------------------------
def build_edges_from_org_list(df: pd.DataFrame, org_col: str = "org_names") -> List[Tuple[str, str, int]]:
    """
    각 행의 org_col(기관명 리스트)에서 모든 무방향 조합을 뽑아 가중치를 집계.
    동일 논문 내 중복 기관은 1회만 세도록 set 처리.
    """
    edges: Dict[Tuple[str, str], int] = {}
    for idx, row in df.iterrows():
        raw = row.get(org_col, None)
        orgs = [o for o in _safe_literal_list(raw) if isinstance(o, str) and o and o.lower() != "none"]
        # 같은 논문 내 중복 제거
        orgs = _unique_preserve(orgs)
        if len(orgs) < 2:
            continue
        # 모든 쌍
        for i in range(len(orgs)):
            for j in range(i + 1, len(orgs)):
                a = orgs[i].strip()
                b = orgs[j].strip()
                if not a or not b or a == b:
                    continue
                u, v = sorted((a, b))
                edges[(u, v)] = edges.get((u, v), 0) + 1
    # dict -> list
    return [(u, v, w) for (u, v), w in edges.items()]


# -----------------------------
# 그래프 필터/트리밍
# -----------------------------
def auto_min_edge_from_quantile(weights: List[int], q: float) -> int:
    if not weights:
        return 1
    s = sorted(weights)
    pos = int(round((len(s) - 1) * q))
    pos = min(max(pos, 0), len(s) - 1)
    thresh = s[pos]
    return max(int(thresh), 1)


def trim_graph(G: nx.Graph,
               min_edge: Optional[int] = None,
               use_quantile: bool = True,
               q: float = 0.85,
               top_k: Optional[int] = 400,
               lcc_only: bool = True,
               kcore: int = 0) -> nx.Graph:
    """
    - min_edge: 엣지 가중치 임계값(없으면 auto)
    - use_quantile: auto 계산 시 상위 q 분위수 사용
    - top_k: 상위 노드(가중치 degree 기준)만 남김
    - lcc_only: 최대 연결성분만 유지
    - kcore: k-core 필터 (0이면 미사용)
    """
    H = G.copy()

    # 엣지 가중치 임계
    weights = [d.get("weight", 1) for _, _, d in H.edges(data=True)]
    if min_edge is None:
        if use_quantile:
            min_edge_eff = auto_min_edge_from_quantile(weights, q)
        else:
            min_edge_eff = 1
    else:
        min_edge_eff = max(int(min_edge), 1)

    if min_edge_eff > 1:
        to_remove = [(u, v) for u, v, d in H.edges(data=True) if d.get("weight", 1) < min_edge_eff]
        H.remove_edges_from(to_remove)

    # 고립노드 제거
    H.remove_nodes_from([n for n, d in H.degree() if d == 0])

    # k-core
    if kcore and H.number_of_nodes() > 0:
        try:
            H = nx.k_core(H, k=kcore)
        except nx.NetworkXError:
            pass

    # LCC만
    if lcc_only and H.number_of_nodes() > 0:
        try:
            comps = list(nx.connected_components(H))
            if comps:
                largest = max(comps, key=len)
                H = H.subgraph(largest).copy()
        except Exception:
            pass

    # top_k 노드(가중치 degree)만
    if top_k and H.number_of_nodes() > top_k:
        strengths = {n: sum(d.get("weight", 1) for _, _, d in H.edges(n, data=True)) for n in H.nodes()}
        top_nodes = sorted(strengths, key=lambda x: strengths[x], reverse=True)[:top_k]
        H = H.subgraph(top_nodes).copy()

    # 마지막 정리: 엣지 없으면 노드 비우기
    if H.number_of_edges() == 0:
        H.clear()

    return H


# -----------------------------
# 레이아웃
# -----------------------------
def compute_layout(G: nx.Graph, layout: str = "fr", seed: int = 42) -> Dict[Any, Tuple[float, float]]:
    """
    layout: 'fr'(fruchterman), 'spring', 'kamada', 'random'
    """
    if G.number_of_nodes() == 0:
        return {}

    layout = (layout or "fr").lower()
    try:
        if layout in ("fr", "fruchterman", "fruchterman_reingold"):
            pos = nx.fruchterman_reingold_layout(G, seed=seed, weight="weight")
        elif layout in ("spring", "force", "forceatlas2", "force_atlas2"):
            pos = nx.spring_layout(G, seed=seed, weight="weight")
        elif layout in ("kamada", "kk"):
            pos = nx.kamada_kawai_layout(G, weight="weight")
        elif layout in ("random",):
            pos = nx.random_layout(G, seed=seed)
        else:
            pos = nx.fruchterman_reingold_layout(G, seed=seed, weight="weight")
        return {n: (float(x), float(y)) for n, (x, y) in pos.items()}
    except Exception:
        # 레이아웃 실패 시 위치 없이 반환
        return {}


# -----------------------------
# Pyvis 변환
# -----------------------------
def to_pyvis(G: nx.Graph, html_path: str,
             scale: float = 4.0,
             base_size: float = 6.0,
             physics: bool = False,
             positions: Optional[Dict[Any, Tuple[float, float]]] = None,
             centrality: Optional[Dict[Any, Dict[str, float]]] = None,
             size_by: str = "strength",
             color_by: Optional[str] = None,
             edge_color_by: Optional[str] = None,
             edge_color_agg: str = "mean"
             ) -> str:
    """
    G를 Pyvis Network로 변환 후 html 저장.
    positions 가 주어지면 물리 시뮬 없이 고정 좌표 사용.
    """
    net = Network(height="800px", width="100%", notebook=False,
                  bgcolor="#FFFFFF", font_color="#000000", directed=False,
                  cdn_resources="in_line")

    # 옵션(JSON) - json.dumps 로 안전하게 전달
    options = {
        "nodes": {
            "shape": "dot",
            "font": {"size": 0},
            "scaling": {"min": 2, "max": 40}
        },
        "edges": {
            "smooth": False,
            "color": {"opacity": 0.8},
            "selectionWidth": 1,
            "hoverWidth": 0
        },
        "interaction": {
            "hover": True,
            "tooltipDelay": 50,
            "hideEdgesOnDrag": True,
            "hideEdgesOnZoom": True,
            "zoomView": True
        },
        "physics": {
            "enabled": bool(physics),
            "solver": "forceAtlas2Based",
            "forceAtlas2Based": {
                "gravitationalConstant": -50,
                "centralGravity": 0.005,
                "springLength": 120,
                "springConstant": 0.08,
                "damping": 0.4,
                "avoidOverlap": 0.6
            },
            "stabilization": {"enabled": True, "iterations": 1000, "updateInterval": 25, "fit": True},
            "minVelocity": 1.0,     
            "timestep": 0.5         
        }
    }
    net.set_options(json.dumps(options))

    if G.number_of_nodes() == 0:
        # 빈 그래프인 경우, 안내 문구만 들어간 HTML 생성
        net.add_node("No data", label="No collaborations after filtering", size=15)
        html_str = net.generate_html() # html 문자열 생성
        if html_path:
            Path(html_path).parent.mkdir(parents=True, exist_ok=True)
            Path(html_path).write_text(html_str, encoding="utf-8")
        return html_str

    # 노드 크기 계산(가중치 degree 기반 sqrt 스케일)
    strengths = {n: sum(d.get("weight", 1) for _, _, d in G.edges(n, data=True)) for n in G.nodes()}
    

    # 색상 매핑용 간단 유틸(분위수 단색 스케일)
    def _color_for(val, lo, hi):
        if hi <= lo:
            return "#4a90e2"
        t = max(0.0, min(1.0, (val - lo) / (hi - lo)))
        r = int(200 - t * 150)  # 200 → 50
        g = int(220 - t * 170)  # 220 → 50
        b = int(255 - t * 205)  # 255 → 50
        return f"#{r:02x}{g:02x}{b:02x}"

    # 색상 기준 범위
    def _range(vals):
        if not vals: return (0.0, 1.0)
        return (min(vals), max(vals))

    color_vals = None
    if color_by:
        if color_by == "strength":
            color_vals = strengths
        elif centrality is not None:
            color_vals = {n: centrality.get(n, {}).get(color_by, 0.0) for n in G.nodes()}
        else:
            color_vals = {n: 0.0 for n in G.nodes()}
        lo, hi = _range(list(color_vals.values()))

    for n in G.nodes():
        w = strengths.get(n, 1)

        # --- 크기 기준 선택 ---
        if size_by == "strength" or (centrality is None and size_by != "strength"):
            base_val = float(w)
        else:
            base_val = float(centrality.get(n, {}).get(size_by, 0.0)) if centrality else float(w)
        size = base_size + scale * (max(base_val, 1e-12) ** 0.5)

        label = n if len(str(n)) <= 24 else (str(n)[:21] + "…")

        # --- 툴팁: 중심성 수치 노출 ---
        if centrality is not None and n in centrality:
            c = centrality[n]
            title = (f"{n}<br/>strength={w}"
                    f"<br/>deg={c.get('degree',0.0):.4f}"
                    f"&nbsp;betw={c.get('betweenness',0.0):.4f}"
                    f"&nbsp;clos={c.get('closeness',0.0):.4f}"
                    f"&nbsp;eig={c.get('eigenvector',0.0):.4f}")
        else:
            title = f"{n}<br/>strength={w}"

        # --- 색상(선택) ---
        node_color = None
        if color_by:
            v = color_vals.get(n, 0.0) if color_vals else 0.0
            node_color = _color_for(v, lo, hi)

        net.add_node(n, label=label, title=title, value=w, size=size,
                    color=node_color if node_color else None)
        

    # 엣지 추가
    for u, v, d in G.edges(data=True):
        w = int(d.get("weight", 1))
        kwargs = {"value": w, "title": str(w)}

        # 중심성 기반 엣지 색상
        if edge_color_by and centrality is not None:
            cu = centrality.get(u, {}).get(edge_color_by, 0.0)
            cv = centrality.get(v, {}).get(edge_color_by, 0.0)
            if edge_color_agg == "min":
                ev = min(cu,cv)
            elif edge_color_agg == "max":
                ev = max(cu,cv)
            else:
                ev = 0.5 * (cu + cv)
            #스케일 계산
            vals = [centrality[n].get(edge_color_by, 0.0) for n in G.nodes() if n in centrality]
            lo2, hi2 = (min(vals), max(vals)) if vals else (0.0, 1.0)
            kwargs["color"] = _color_for(ev, lo2, hi2)

        net.add_edge(u, v, **kwargs)

    # 좌표 고정(옵션)
    if positions:
        try:
            # pyvis에서 physics=False일 때 x,y를 사용하면 위치 고정됨
            for n, (x, y) in positions.items():
                if n in net.node_ids:
                    i = net.node_ids.index(n)
                    net.nodes[i]["x"] = float(x) * 1000.0
                    net.nodes[i]["y"] = float(y) * 1000.0
                    net.nodes[i]["fixed"] = True
        except Exception:
            pass

    html_str = net.generate_html()

    # 안정화 끝나면 physics off
    html_str = html_str.replace(
        "network = new vis.Network(container, data, options);",
        "network = new vis.Network(container, data, options);\n"
        "network.once('stabilizationIterationsDone', function () {\n"
        "  network.stopSimulation();\n"
        "  network.setOptions({ physics: false });\n"
        "});"
    )
    if html_path:
        Path(html_path).parent.mkdir(parents=True, exist_ok=True)
        Path(html_path).write_text(html_str, encoding="utf-8")
    return html_str


# -----------------------------
# 엔드 투 엔드
# -----------------------------
def main(input_csv: str,
         output_html: Optional[str] = None,
         min_edge: Optional[int] = None,
         use_quantile: bool = True,
         q: float = 0.85,
         kcore: int = 0,
         lcc_only: bool = True,
         top_k: Optional[int] = 400,
         layout: str = "fr",
         physics: bool = True,
         size_by: str = "strength",
         color_by: Optional[str] = None,
         edge_color_by: Optional[str] = None,
         edge_color_agg: str = "mean",
         save_metrics: bool = True,
         save_top10_md: bool = True
         ) -> Optional[str]:
    
    """
    input_csv: real5 단계 결과(csv)에 org_names 컬럼이 있어야 빠름(권장).
               없으면 ror 를 기반으로 단순한 이름을 만들어 사용(품질 저하).
    반환: 생성된 HTML 경로(성공 시)
    """

    try:
        df = pd.read_csv(Path(input_csv), low_memory=False)
    except Exception as e:
        print(f"[visualize_fast] CSV 읽기 실패: {e}")
        return None

    org_col = "org_names" if "org_names" in df.columns else None
    if org_col is None and "ror" in df.columns:
        # ror 기반 fallback 이름 생성
        def ror_to_name(cell: Any) -> List[str]:
            ids = []
            for x in _safe_literal_list(cell):
                if not isinstance(x, str):
                    continue
                s = x.strip()
                if not s:
                    continue
                # 예: https://ror.org/03yrm5c26 -> 03yrm5c26
                if s.startswith("http"):
                    base = s.rstrip("/").split("/")[-1]
                else:
                    base = s
                ids.append(f"ROR:{base}")
            return ids
        df["org_names"] = df["ror"].apply(ror_to_name)
        org_col = "org_names"

    if org_col is None:
        print("[visualize_fast] org_names/ror 컬럼이 없어 시각화를 건너뜁니다.")
        return None

    # 엣지 구성
    edges = build_edges_from_org_list(df, org_col=org_col)
    print(f"[visualize_fast] 원시 엣지 수: {len(edges)}")

    G = nx.Graph()
    for u, v, w in edges:
        if G.has_edge(u, v):
            G[u][v]["weight"] += int(w)
        else:
            G.add_edge(u, v, weight=int(w))

    # 트리밍
    Gt = trim_graph(G,
                    min_edge=min_edge,
                    use_quantile=use_quantile,
                    q=q,
                    top_k=top_k,
                    lcc_only=lcc_only,
                    kcore=kcore)

    print(f"[visualize_fast] 필터 후 노드: {Gt.number_of_nodes()}, 엣지: {Gt.number_of_edges()}")


    centrality = {}
    if Gt.number_of_nodes() > 0:
        degree_c   = nx.degree_centrality(Gt)
        between_c  = nx.betweenness_centrality(Gt, weight="weight", normalized=True)
        close_c    = nx.closeness_centrality(Gt)
        try:
            eigen_c = nx.eigenvector_centrality(Gt, weight="weight", max_iter=1000)
        except Exception:
            eigen_c = {n: 0.0 for n in Gt.nodes()}
        for n in Gt.nodes():
            centrality[n] = {
                "degree": degree_c.get(n, 0.0),
                "betweenness": between_c.get(n, 0.0),
                "closeness": close_c.get(n, 0.0),
                "eigenvector": eigen_c.get(n, 0.0),
            }

        if save_metrics:
            import csv
            centrality_csv = (os.path.splitext(str(input_csv))[0] + "_centrality.csv")
            with open(centrality_csv, "w", newline="", encoding="utf-8") as fp:
                w = csv.writer(fp)
                w.writerow(["org","degree","betweenness","closeness","eigenvector"])
                for org in sorted(centrality.keys(), key=lambda x: x.lower()):
                    c = centrality[org]
                    w.writerow([org, c["degree"], c["betweenness"], c["closeness"], c["eigenvector"]])
            print(f"[visualize_fast] 중앙성 CSV 저장: {centrality_csv}")
            if save_top10_md:
                # 간단한 Top10 Markdown
                def _md_table(metric: str, k: int = 10) -> str:
                    top = sorted(((n, centrality[n][metric]) for n in centrality), key=lambda p: p[1], reverse=True)[:k]
                    lines = [f"### {metric.capitalize()} Centrality (Top 10)",
                            "| Rank | Organization | Score |",
                            "|---:|:-------------|-----:|"]
                    for i,(org,sc) in enumerate(top,1):
                        lines.append(f"| {i} | {org} | {sc:.6f} |")
                    return "\n".join(lines)
                md_path = os.path.splitext(str(input_csv))[0] + "_centrality_top10.md"
                md = "# Centrality Top 10\n\n" + "\n\n".join([
                    _md_table("degree"), _md_table("betweenness"), _md_table("closeness"), _md_table("eigenvector")
                ]) + "\n"
                with open(md_path, "w", encoding="utf-8") as fp:
                    fp.write(md)
                print(f"[visualize_fast] 중앙성 Top10 Markdown 저장: {md_path}")



    # 레이아웃(physics=False 기본, 좌표 선계산)
    pos = compute_layout(Gt, layout=layout or "fr", seed=42) if not physics else {}

    # 파일명
    html_path = output_html or (os.path.splitext(str(input_csv))[0] + "_network_fast.html")
    html_out = to_pyvis(Gt, html_path,
                    scale=4.0, base_size=6.0,
                    physics=physics, positions=pos,
                    centrality=centrality if centrality else None,
                    size_by=size_by,
                    color_by=color_by,
                    edge_color_by=edge_color_by,
                    edge_color_agg=edge_color_agg
                    )
    
    return html_path if output_html else html_out


if __name__ == "__main__":
    # 모듈 단독 실행 테스트(필요 시 경로 수정)
    import sys
    if len(sys.argv) >= 2:
        input_csv = sys.argv[1]
        output_html = sys.argv[2] if len(sys.argv) >= 3 else None
        main(input_csv=input_csv, output_html=output_html)
    else:
        print("Usage: python real6_visualize_fast.py <input_csv> [output_html]")
