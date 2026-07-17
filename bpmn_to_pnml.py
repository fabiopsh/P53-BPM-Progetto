#!/usr/bin/env python3
"""
Convertitore BPMN (orchestration, singolo pool) -> rete di workflow in formato
PNML compatibile con WoPeD.

Regole di traduzione (derivate e validate confrontando l'output con
Petri Nets/alex.pnml, gia' modellata a mano da Fabio in WoPeD):

- task/sendTask/receiveTask/userTask/..., startEvent, endEvent,
  intermediateCatchEvent, intermediateThrowEvent, parallelGateway (AND)
  sono nodi "hard": diventano SEMPRE una transizione.
- exclusiveGateway (XOR) ed eventBasedGateway sono nodi "soft": NON
  diventano una transizione. Un gruppo di nodi soft direttamente connessi
  puo' condividere UN SOLO posto, ma SOLO quando cio' non introduce
  raggiungibilita' spuria. Per un arco soft->soft (A -> B) la fusione in
  un unico posto e' sicura se e solo se:
      out_degree(A) == 1   oppure   in_degree(B) == 1
  (cioe' almeno uno dei due lati e' un "pass-through" puro rispetto a
  quell'arco). Se nessuna delle due condizioni vale (es. uno split reale
  seguito da un join reale, senza che l'uno sia l'unica via dell'altro),
  la fusione introdurrebbe archi falsi (un token che non dovrebbe poter
  raggiungere un certo successore lo raggiungerebbe tramite il posto
  condiviso). In quel caso si inserisce una transizione di "relay"
  (1 posto in ingresso, 1 in uscita) che materializza esplicitamente quel
  ramo, esattamente come fatto a mano da Fabio per alex.pnml
  (es. "t_manutenzione_no", "t_annullato_si", "t_tipo_proposta_ricevuta").
- Un arco diretto hard->hard (nessun gateway in mezzo) genera comunque un
  posto "banale" (un posto per quell'arco).
- startEvent: gli viene anteposto un posto iniziale univoco (marcatura 1).
- endEvent: gli viene posposto un posto finale univoco (nessun arco uscente).
- Gli intermediate link event (throw/catch con lo stesso nome, usati per i
  loop-back "a goto") vengono accoppiati per nome e trattati come un arco
  hard->hard implicito (non essendoci un vero sequenceFlow tra i due).

Uso:
    python3 bpmn_to_pnml.py alex.bpmn alex_gen.pnml "Alex - Rete di Petri (generata)"
"""
import sys
import xml.etree.ElementTree as ET
from collections import deque

BPMN_NS = "http://www.omg.org/spec/BPMN/20100524/MODEL"
NS = {"bpmn": BPMN_NS}

HARD_TAGS = {
    "task", "sendTask", "receiveTask", "userTask", "serviceTask",
    "scriptTask", "businessRuleTask", "manualTask",
    "startEvent", "endEvent", "intermediateCatchEvent", "intermediateThrowEvent",
    "parallelGateway",
}
SOFT_TAGS = {"exclusiveGateway", "eventBasedGateway", "inclusiveGateway"}


def slug(text, fallback):
    if not text:
        text = fallback
    out = []
    for ch in text.lower():
        if ch.isalnum():
            out.append(ch)
        elif ch in " _-/'":
            out.append("_")
    s = "".join(out).strip("_")
    while "__" in s:
        s = s.replace("__", "_")
    return s or fallback


class BpmnProcess:
    def __init__(self, bpmn_path):
        self.tree = ET.parse(bpmn_path)
        self.root = self.tree.getroot()
        proc = self.root.find(".//bpmn:process", NS)
        if proc is None:
            raise ValueError("Nessun <bpmn:process> trovato in " + bpmn_path)
        self.process = proc
        self.nodes = {}       # id -> {tag, name, kind}
        self.out_edges = {}   # id -> [(target_id, flow_name)]
        self.in_edges = {}    # id -> [(source_id, flow_name)]
        self._parse()
        self._pair_link_events()

    def _parse(self):
        for el in self.process.iter():
            tag = el.tag.split("}")[-1]
            eid = el.get("id")
            if eid is None or tag == "sequenceFlow":
                continue
            if tag in HARD_TAGS or tag in SOFT_TAGS:
                self.nodes[eid] = {
                    "tag": tag,
                    "name": el.get("name"),
                    "kind": "hard" if tag in HARD_TAGS else "soft",
                    "el": el,
                }
                self.out_edges.setdefault(eid, [])
                self.in_edges.setdefault(eid, [])
        for sf in self.process.findall("bpmn:sequenceFlow", NS):
            src = sf.get("sourceRef")
            tgt = sf.get("targetRef")
            name = sf.get("name")
            if src in self.nodes and tgt in self.nodes:
                self.out_edges.setdefault(src, []).append((tgt, name))
                self.in_edges.setdefault(tgt, []).append((src, name))

    def _pair_link_events(self):
        """Collega (in modo sintetico) gli intermediateThrowEvent con
        linkEventDefinition al corrispondente intermediateCatchEvent con lo
        stesso nome, dato che nel BPMN non esiste un sequenceFlow esplicito
        tra i due."""
        throws, catches = [], []
        for nid, n in self.nodes.items():
            has_link = n["el"].find("bpmn:linkEventDefinition", NS) is not None
            if not has_link:
                continue
            if n["tag"] == "intermediateThrowEvent":
                throws.append(nid)
            elif n["tag"] == "intermediateCatchEvent":
                catches.append(nid)
        for tid in throws:
            tname = self.nodes[tid]["name"]
            for cid in catches:
                if self.nodes[cid]["name"] == tname:
                    self.out_edges[tid].append((cid, None))
                    self.in_edges[cid].append((tid, None))

    def node_label(self, nid):
        n = self.nodes[nid]
        return n["name"] or n["tag"]

    def nearest_hard_predecessor_labels(self, nid, _seen=None):
        """Risale all'indietro attraverso i soli nodi soft (gateway) fino ai
        piu' vicini nodi hard, per usare le loro etichette come fallback
        quando un gateway senza nome finirebbe altrimenti per esporre il suo
        tag grezzo (es. "exclusiveGateway")."""
        if _seen is None:
            _seen = set()
        if nid in _seen:
            return []
        _seen.add(nid)
        labels = []
        for src, _ in self.in_edges.get(nid, []):
            if self.nodes[src]["kind"] == "hard":
                labels.append(self.node_label(src))
            else:
                labels.extend(self.nearest_hard_predecessor_labels(src, _seen))
        return labels


class UnionFind:
    def __init__(self, items):
        self.parent = {x: x for x in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


class NetBuilder:
    def __init__(self, proc, prefix=""):
        self.proc = proc
        self.prefix = prefix
        self.places = {}
        self.transitions = {}
        self.arcs = []
        self.initial_place = None
        self.final_places = []
        self._place_ids = set()
        self._trans_ids = set()
        self.node_to_transition = {}
        self.group_place = {}
        self.edge_place = {}
        self.relay_transition = {}   # (src,tgt) unsafe soft-soft edge -> transition id

    def _uid(self, base, used):
        cand = base
        i = 2
        while cand in used:
            cand = base + "_" + str(i)
            i += 1
        used.add(cand)
        return cand

    def _new_place(self, base_label):
        pid = self._uid(self.prefix + "p_" + slug(base_label, "x"), self._place_ids)
        self.places[pid] = base_label
        return pid

    def _new_transition(self, base_label):
        tid = self._uid(self.prefix + "t_" + slug(base_label, "x"), self._trans_ids)
        self.transitions[tid] = base_label
        return tid

    def build(self):
        proc = self.proc
        soft_ids = [nid for nid, n in proc.nodes.items() if n["kind"] == "soft"]
        uf = UnionFind(soft_ids)

        unsafe_edges = []
        for src in soft_ids:
            out_deg = len(proc.out_edges.get(src, []))
            for tgt, _ in proc.out_edges.get(src, []):
                if proc.nodes[tgt]["kind"] != "soft":
                    continue
                in_deg = len(proc.in_edges.get(tgt, []))
                if out_deg == 1 or in_deg == 1:
                    uf.union(src, tgt)
                else:
                    unsafe_edges.append((src, tgt))

        # 1) una transizione per ogni nodo hard
        DEFAULT_LABELS = {"startEvent": "Inizio processo", "endEvent": "Fine processo"}
        for nid, n in proc.nodes.items():
            if n["kind"] != "hard":
                continue
            label = n["name"] or DEFAULT_LABELS.get(n["tag"], n["tag"])
            has_link = n["el"].find("bpmn:linkEventDefinition", NS) is not None
            if has_link and n["name"]:
                if n["tag"] == "intermediateCatchEvent":
                    label = n["name"] + " (catch)"
                elif n["tag"] == "intermediateThrowEvent":
                    label = n["name"] + " (throw)"
            self.node_to_transition[nid] = self._new_transition(label)

        # 2) una transizione di relay per ogni arco soft->soft "non sicuro"
        for src, tgt in unsafe_edges:
            if proc.nodes[src]["name"]:
                src_label = proc.nodes[src]["name"]
            else:
                preds = proc.nearest_hard_predecessor_labels(src)
                src_label = " / ".join(dict.fromkeys(preds)) if preds else proc.node_label(src)
            branch = None
            for t, fname in proc.out_edges[src]:
                if t == tgt and fname:
                    branch = fname
                    break
            if branch:
                label = src_label + " (" + branch + ")"
            else:
                label = src_label + " -> " + proc.node_label(tgt)
            self.relay_transition[(src, tgt)] = self._new_transition(label)

        # 3) un posto per ogni gruppo (fuso) di nodi soft
        groups = {}
        for nid in soft_ids:
            groups.setdefault(uf.find(nid), []).append(nid)
        unnamed_places = []  # posti senza un nome BPMN: da rietichettare al passo 6
        for root, members in groups.items():
            named = [proc.nodes[m]["name"] for m in members if proc.nodes[m]["name"]]
            if named:
                label = " / ".join(dict.fromkeys(named))
            else:
                label = "snodo"  # placeholder temporaneo, sostituito al passo 6
            pid = self._new_place(label)
            self.group_place[root] = pid
            if not named:
                unnamed_places.append(pid)

        def place_of(soft_id):
            return self.group_place[uf.find(soft_id)]

        # 4) posto iniziale e posto finale
        start_nodes = [nid for nid, n in proc.nodes.items() if n["tag"] == "startEvent"]
        end_nodes = [nid for nid, n in proc.nodes.items() if n["tag"] == "endEvent"]
        self.initial_place = self._new_place("Inizio")
        for nid in start_nodes:
            self.arcs.append((self.initial_place, self.node_to_transition[nid]))
        final_place = self._new_place("Fine")
        self.final_places.append(final_place)
        for nid in end_nodes:
            self.arcs.append((self.node_to_transition[nid], final_place))

        # 5) tutti gli altri archi, classificando ogni sequenceFlow originale
        seen_edges = set()
        for src, n in proc.nodes.items():
            for tgt, _fname in proc.out_edges.get(src, []):
                key = (src, tgt)
                if key in seen_edges:
                    continue
                seen_edges.add(key)
                src_kind = proc.nodes[src]["kind"]
                tgt_kind = proc.nodes[tgt]["kind"]

                if src_kind == "hard" and tgt_kind == "hard":
                    pid = self._edge_place(src, tgt)
                    self.arcs.append((self.node_to_transition[src], pid))
                    self.arcs.append((pid, self.node_to_transition[tgt]))
                elif src_kind == "hard" and tgt_kind == "soft":
                    self.arcs.append((self.node_to_transition[src], place_of(tgt)))
                elif src_kind == "soft" and tgt_kind == "hard":
                    self.arcs.append((place_of(src), self.node_to_transition[tgt]))
                else:  # soft -> soft
                    if key in self.relay_transition:
                        rt = self.relay_transition[key]
                        self.arcs.append((place_of(src), rt))
                        self.arcs.append((rt, place_of(tgt)))
                    # se sicuro (stesso gruppo), non serve alcun arco: e' assorbito

        # 6) rietichetta i posti-gruppo senza nome BPMN usando le transizioni
        #    a cui sono collegati, invece di esporre l'id interno del BPMN
        #    (es. "Gateway_0g7xn4a") che non dice nulla del contenuto.
        preds = {}
        succs = {}
        for s, t in self.arcs:
            if t in self.transitions:
                preds.setdefault(t, [])
            if s in self.transitions:
                succs.setdefault(s, [])
        for s, t in self.arcs:
            if s in unnamed_places and t in self.transitions:
                succs.setdefault(s, []).append(self.transitions[t])
            if t in unnamed_places and s in self.transitions:
                preds.setdefault(t, []).append(self.transitions[s])
        for pid in unnamed_places:
            pred_labels = list(dict.fromkeys(preds.get(pid, [])))
            succ_labels = list(dict.fromkeys(succs.get(pid, [])))
            if pred_labels:
                label = " / ".join(pred_labels)
            elif succ_labels:
                label = " / ".join(succ_labels)
            else:
                label = "Snodo interno"
            if len(label) > 80:
                label = label[:77] + "..."
            self.places[pid] = label
        return self

    def _edge_place(self, src, tgt):
        key = (src, tgt)
        if key in self.edge_place:
            return self.edge_place[key]
        # src/tgt sono sempre nodi hard qui: riusiamo l'etichetta gia'
        # calcolata per la loro transizione (include "Inizio/Fine processo"
        # e i suffissi (catch)/(throw)) invece di richiamare proc.node_label,
        # che per un nodo senza nome esporrebbe il tag BPMN grezzo.
        src_label = self.transitions.get(self.node_to_transition.get(src), None) or self.proc.node_label(src)
        tgt_label = self.transitions.get(self.node_to_transition.get(tgt), None) or self.proc.node_label(tgt)
        label = src_label + " -> " + tgt_label
        pid = self._new_place(label)
        self.edge_place[key] = pid
        return pid


def layered_layout(places, transitions, arcs, x_gap=150, y_gap=110):
    """Layout a livelli (stile Sugiyama) pensato per reti con cicli:
    1) individua e rimuove (solo ai fini del layout, non della rete reale)
       gli archi all'indietro con una DFS, cosi' i loop di retry del
       processo non rompono l'ordinamento a livelli;
    2) assegna i livelli con un vero ordinamento topologico (Kahn) sul
       grafo aciclico risultante, cosi' un nodo e' sempre posizionato dopo
       TUTTI i suoi predecessori "in avanti";
    3) per ogni arco che "salta" piu' di un livello, inserisce una catena
       di nodi fittizi (uno per livello intermedio) usati SOLO per
       l'ordinamento: cosi' quell'arco si riserva una corsia verticale
       propria invece di tagliare in diagonale attraverso nodi non
       correlati, che e' la causa principale delle sovrapposizioni viste
       in WoPeD;
    4) dentro ogni livello (nodi reali + fittizi) riordina col metodo del
       baricentro per ridurre gli incroci tra gli archi;
    5) centra verticalmente ogni livello rispetto al piu' popolato, e usa
       le posizioni (con gli eventuali "buchi" lasciati dai nodi fittizi)
       solo per i nodi reali, che sono gli unici scritti nel PNML.
    """
    all_nodes = list(places.keys()) + list(transitions.keys())
    node_set = set(all_nodes)
    adj = {n: [] for n in all_nodes}
    radj = {n: [] for n in all_nodes}
    for s, t in arcs:
        if s in node_set and t in node_set:
            adj[s].append(t)
            radj[t].append(s)

    # --- 1) DFS iterativa per individuare gli archi all'indietro (cicli) ---
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n: WHITE for n in all_nodes}
    back_edges = set()

    def dfs_from(root):
        if color[root] != WHITE:
            return
        stack = [(root, iter(adj[root]))]
        color[root] = GRAY
        while stack:
            node, it = stack[-1]
            advanced = False
            for nxt in it:
                if color[nxt] == WHITE:
                    color[nxt] = GRAY
                    stack.append((nxt, iter(adj[nxt])))
                    advanced = True
                    break
                elif color[nxt] == GRAY:
                    back_edges.add((node, nxt))
            if not advanced:
                color[node] = BLACK
                stack.pop()

    roots = [n for n in all_nodes if not radj[n]] or all_nodes[:1]
    for r in roots:
        dfs_from(r)
    for n in all_nodes:
        dfs_from(n)  # eventuali componenti non raggiunte dai root

    # --- 2) livelli via ordinamento topologico sul grafo senza i back edge ---
    dag_adj = {n: [] for n in all_nodes}
    indeg = {n: 0 for n in all_nodes}
    for s, t in arcs:
        if s not in node_set or t not in node_set or (s, t) in back_edges:
            continue
        dag_adj[s].append(t)
        indeg[t] += 1

    layer = {n: 0 for n in all_nodes}
    work = dict(indeg)
    q = deque([n for n in all_nodes if work[n] == 0])
    while q:
        n = q.popleft()
        for m in dag_adj[n]:
            layer[m] = max(layer[m], layer[n] + 1)
            work[m] -= 1
            if work[m] == 0:
                q.append(m)

    # --- 3) nodi fittizi per gli archi che saltano piu' di un livello ---
    # Il grafo di ordinamento (ord_adj/ord_radj) include, oltre ai nodi
    # reali, una catena di nodi fittizi per ogni arco (in qualunque
    # direzione, anche i back edge) che collega livelli non adiacenti: cosi'
    # quell'arco "occupa" una corsia verticale in ogni livello che
    # attraversa, e il baricentro sposta i nodi reali per lasciargliela
    # libera invece di farci passare sopra.
    ord_adj = {n: [] for n in all_nodes}
    ord_radj = {n: [] for n in all_nodes}
    dummy_layer = {}
    dummy_count = 0
    seen_pairs = set()
    for s, t in arcs:
        if s not in node_set or t not in node_set or s == t:
            continue
        if (s, t) in seen_pairs:
            continue
        seen_pairs.add((s, t))
        a, b = (s, t) if layer[s] <= layer[t] else (t, s)
        la, lb = layer[a], layer[b]
        if lb - la <= 1:
            ord_adj[a].append(b)
            ord_radj[b].append(a)
            continue
        prev = a
        for lyr in range(la + 1, lb):
            dummy_count += 1
            d = "__dummy_" + str(dummy_count)
            dummy_layer[d] = lyr
            ord_adj[d] = []
            ord_radj[d] = []
            ord_adj[prev].append(d)
            ord_radj[d].append(prev)
            prev = d
        ord_adj[prev].append(b)
        ord_radj[b].append(prev)

    by_layer = {}
    for n in all_nodes:
        by_layer.setdefault(layer[n], []).append(n)
    for d, lyr in dummy_layer.items():
        by_layer.setdefault(lyr, []).append(d)
    max_layer = max(by_layer) if by_layer else 0
    for nodes in by_layer.values():
        nodes.sort()
    order_in_layer = {n: i for nodes in by_layer.values() for i, n in enumerate(nodes)}

    # --- 4) riduzione incroci col metodo del baricentro (alcune passate) ---
    def barycenter_pass(layers_seq, neighbor_fn):
        for lyr in layers_seq:
            nodes = by_layer.get(lyr, [])
            if not nodes:
                continue
            scores = {}
            for n in nodes:
                positions = [order_in_layer[m] for m in neighbor_fn(n) if m in order_in_layer]
                scores[n] = (sum(positions) / len(positions)) if positions else order_in_layer[n]
            nodes.sort(key=lambda n: scores[n])
            for i, n in enumerate(nodes):
                order_in_layer[n] = i

    for _ in range(4):
        barycenter_pass(range(1, max_layer + 1), lambda n: ord_radj[n])
        barycenter_pass(range(max_layer - 1, -1, -1), lambda n: ord_adj[n])

    # --- 5) coordinate finali (solo nodi reali): livelli centrati
    #        verticalmente, usando l'indice con gli eventuali "buchi"
    #        lasciati dai nodi fittizi per dare spazio agli archi lunghi ---
    max_count = max((len(v) for v in by_layer.values()), default=1)
    layer_offset = {}
    for lyr, nodes in by_layer.items():
        layer_offset[lyr] = (max_count - len(nodes)) * y_gap / 2

    pos = {}
    for n in all_nodes:
        lyr = layer[n]
        pos[n] = (40 + lyr * x_gap, int(40 + layer_offset[lyr] + order_in_layer[n] * y_gap))
    return pos


PNML_HEADER = """<?xml version="1.0" encoding="UTF-8"?>
<!--Generato automaticamente da bpmn_to_pnml.py a partire dal diagramma BPMN.
Compatibile con WoPeD (Workflow PetriNet Designer).-->
<pnml>
  <net type="http://www.informatik.hu-berlin.de/top/pntd/ptNetb" id="{net_id}">
    <name>
      <text>{net_name}</text>
    </name>
"""
PNML_FOOTER = """  </net>
</pnml>
"""


def xml_escape(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
             .replace('"', "&quot;"))


def write_pnml(nb, out_path, net_id, net_name):
    pos = layered_layout(nb.places, nb.transitions, nb.arcs)
    lines = [PNML_HEADER.format(net_id=net_id, net_name=xml_escape(net_name))]

    for pid, label in nb.places.items():
        x, y = pos.get(pid, (0, 0))
        lines.append('    <place id="' + pid + '">')
        lines.append("      <name>")
        lines.append("        <text>" + xml_escape(label) + "</text>")
        lines.append('        <graphics>')
        lines.append('          <offset x="' + str(x + 20) + '" y="' + str(y + 50) + '"/>')
        lines.append('        </graphics>')
        lines.append("      </name>")
        lines.append('      <graphics>')
        lines.append('        <position x="' + str(x) + '" y="' + str(y) + '"/>')
        lines.append('        <dimension x="40" y="40"/>')
        lines.append('      </graphics>')
        if pid == nb.initial_place:
            lines.append("      <initialMarking>")
            lines.append("        <text>1</text>")
            lines.append("      </initialMarking>")
        lines.append("    </place>")

    for tid, label in nb.transitions.items():
        x, y = pos.get(tid, (0, 0))
        lines.append('    <transition id="' + tid + '">')
        lines.append("      <name>")
        lines.append("        <text>" + xml_escape(label) + "</text>")
        lines.append('        <graphics>')
        lines.append('          <offset x="' + str(x + 20) + '" y="' + str(y + 50) + '"/>')
        lines.append('        </graphics>')
        lines.append("      </name>")
        lines.append('      <graphics>')
        lines.append('        <position x="' + str(x) + '" y="' + str(y) + '"/>')
        lines.append('        <dimension x="40" y="40"/>')
        lines.append('      </graphics>')
        lines.append('      <toolspecific tool="WoPeD" version="1.0">')
        lines.append('        <time>0</time>')
        lines.append('        <timeUnit>1</timeUnit>')
        lines.append('        <orientation>1</orientation>')
        lines.append('      </toolspecific>')
        lines.append("    </transition>")

    for i, st in enumerate(nb.arcs, start=1):
        s, t = st
        aid = nb.prefix + "a" + str(i)
        lines.append('    <arc id="' + aid + '" source="' + s + '" target="' + t + '">')
        lines.append('      <inscription>')
        lines.append('        <text>1</text>')
        lines.append('      </inscription>')
        lines.append("    </arc>")

    lines.append(PNML_FOOTER)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def convert(bpmn_path, out_path, net_name, prefix="", net_id="PetriNet"):
    proc = BpmnProcess(bpmn_path)
    nb = NetBuilder(proc, prefix=prefix).build()
    write_pnml(nb, out_path, net_id, net_name)
    return nb


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: bpmn_to_pnml.py input.bpmn output.pnml [nome_rete] [prefix]")
        sys.exit(1)
    bpmn_path = sys.argv[1]
    out_path = sys.argv[2]
    net_name = sys.argv[3] if len(sys.argv) > 3 else "Rete generata"
    prefix = sys.argv[4] if len(sys.argv) > 4 else ""
    nb = convert(bpmn_path, out_path, net_name, prefix=prefix)
    print("Posti: " + str(len(nb.places)) + "  Transizioni: " + str(len(nb.transitions)) + "  Archi: " + str(len(nb.arcs)))
