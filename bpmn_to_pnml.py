#!/usr/bin/env python3
"""
Convertitore BPMN (orchestration a singolo pool, o collaboration a piu' pool
con message flow) -> rete di workflow in formato PNML compatibile con WoPeD.

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
  NOTA IMPORTANTE: questa e' la cosa che fa la differenza rispetto a un
  convertitore "naive" (es. il tool esterno usato per generare la vecchia
  PetriNet/collaboration.pnml): un gateway XOR con N uscite NON diventa mai
  una singola transizione con N archi in uscita (quello sarebbe un AND-split
  mascherato: scattando metterebbe un token su OGNI ramo insieme, invece che
  su uno solo). Qui invece resta un posto condiviso con N transizioni "hard"
  alternative collegate ad esso: solo una puo' scattare per volta, il che e'
  la corretta semantica "scelta libera" (free-choice) di un XOR.
- Un arco diretto hard->hard (nessun gateway in mezzo) genera comunque un
  posto "banale" (un posto per quell'arco).
- startEvent: gli viene anteposto un posto iniziale univoco (marcatura 1).
- endEvent: gli viene posposto un posto finale univoco (nessun arco uscente).
- Gli intermediate link event (throw/catch con lo stesso nome, usati per i
  loop-back "a goto") vengono accoppiati per nome e trattati come un arco
  hard->hard implicito (non essendoci un vero sequenceFlow tra i due).
- Collaboration con piu' <bpmn:process> (piu' pool): ogni processo viene
  tradotto separatamente (con un prefisso di id univoco per pool, derivato
  dal nome del participant), poi le sotto-reti vengono unite in un'unica
  rete di workflow con:
    * un posto/transizione di "avvio collaborazione" che marca con un
      unico token iniziale l'inizio di TUTTI i pool contemporaneamente
      (una vera rete di workflow deve avere un solo posto sorgente);
    * un posto/transizione di "fine collaborazione" (AND-join) raggiunto
      solo quando OGNI pool ha raggiunto il proprio end event (un solo
      posto pozzo finale);
    * un posto per ogni bpmn:messageFlow, che collega la transizione del
      nodo mittente (sendTask/throwEvent, nel suo pool) alla transizione
      del nodo destinatario (receiveTask/catchEvent, nell'altro pool) --
      stesso pattern gia' usato correttamente per gli archi hard->hard
      dentro un singolo pool, solo attraverso il confine fra i due pool.

Uso:
    python3 bpmn_to_pnml.py alex.bpmn alex_gen.pnml "Alex - Rete di Petri (generata)"
    python3 bpmn_to_pnml.py collaboration.bpmn collab_gen.pnml "Collaboration - Rete di Petri (generata)"
(la seconda forma viene rilevata automaticamente quando il file contiene
piu' di un <bpmn:process>: non serve alcuna opzione in piu'.)
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
    def __init__(self, process_el):
        """process_el: l'elemento <bpmn:process> gia' individuato dal
        chiamante (cosi' la stessa classe funziona sia per un file a
        singolo pool sia per un file di collaboration con piu' pool)."""
        self.process = process_el
        self.nodes = {}       # id -> {tag, name, kind}
        self.out_edges = {}   # id -> [(target_id, flow_name)]
        self.in_edges = {}    # id -> [(source_id, flow_name)]
        self._parse()
        self._pair_link_events()

    @classmethod
    def from_file(cls, bpmn_path):
        """Comportamento originale: apre il file e prende il PRIMO
        <bpmn:process> trovato (va bene per un BPMN a singolo pool)."""
        tree = ET.parse(bpmn_path)
        root = tree.getroot()
        proc = root.find(".//bpmn:process", NS)
        if proc is None:
            raise ValueError("Nessun <bpmn:process> trovato in " + bpmn_path)
        return cls(proc)

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
    proc = BpmnProcess.from_file(bpmn_path)
    nb = NetBuilder(proc, prefix=prefix).build()
    write_pnml(nb, out_path, net_id, net_name)
    return nb


class CombinedNet:
    """Contenitore 'finto NetBuilder' che espone la stessa interfaccia
    (places/transitions/arcs/initial_place/prefix) cosi' write_pnml() puo'
    essere riusato identico anche per una collaboration a piu' pool."""

    def __init__(self):
        self.places = {}
        self.transitions = {}
        self.arcs = []
        self.initial_place = None
        self.final_places = []
        self.prefix = ""
        self._ids = set()

    def _uid(self, base):
        cand = base
        i = 2
        while cand in self._ids:
            cand = base + "_" + str(i)
            i += 1
        self._ids.add(cand)
        return cand

    def new_place(self, base_label):
        pid = self._uid("p_" + slug(base_label, "x"))
        self.places[pid] = base_label
        return pid

    def new_transition(self, base_label):
        tid = self._uid("t_" + slug(base_label, "x"))
        self.transitions[tid] = base_label
        return tid

    def absorb(self, nb):
        """Importa posti/transizioni/archi di una sotto-rete (gia' con id
        prefissati per pool, quindi senza collisioni) cosi' come sono."""
        self.places.update(nb.places)
        self.transitions.update(nb.transitions)
        self.arcs.extend(nb.arcs)
        self._ids.update(nb.places.keys())
        self._ids.update(nb.transitions.keys())


def convert_collaboration(bpmn_path, out_path, net_name, net_id="PetriNet"):
    """Traduce un file BPMN di collaboration (piu' <bpmn:process>, uniti da
    <bpmn:messageFlow>) in un'unica rete di workflow PNML. Ogni pool viene
    tradotto con le stesse regole free-choice del caso a singolo pool
    (vedi NetBuilder), poi le sotto-reti sono unite aggiungendo:
    un solo posto/transizione di avvio, uno di fine (AND-join su tutti i
    pool), e un posto per ogni messageFlow fra un pool e l'altro."""
    tree = ET.parse(bpmn_path)
    root = tree.getroot()
    collab_el = root.find("bpmn:collaboration", NS)
    proc_els = root.findall("bpmn:process", NS)

    if len(proc_els) < 2 or collab_el is None:
        # non e' davvero una collaboration multi-pool: usa il percorso classico
        return convert(bpmn_path, out_path, net_name, net_id=net_id)

    # nome del participant (per un prefisso di id leggibile) per ogni processRef
    proc_id_to_pname = {}
    for part in collab_el.findall("bpmn:participant", NS):
        pref = part.get("processRef")
        if pref:
            proc_id_to_pname[pref] = part.get("name") or pref

    message_flows = [
        (mf.get("id"), mf.get("sourceRef"), mf.get("targetRef"))
        for mf in collab_el.findall("bpmn:messageFlow", NS)
    ]

    combined = CombinedNet()
    node_to_transition_global = {}   # id nodo BPMN (di qualunque pool) -> id transizione interna
    sub_builders = []
    used_prefixes = set()

    for proc_el in proc_els:
        pid = proc_el.get("id")
        pname = proc_id_to_pname.get(pid, pid)
        base_prefix = slug(pname, pid) + "_"
        prefix = base_prefix
        i = 2
        while prefix in used_prefixes:
            prefix = base_prefix.rstrip("_") + str(i) + "_"
            i += 1
        used_prefixes.add(prefix)

        proc = BpmnProcess(proc_el)
        nb = NetBuilder(proc, prefix=prefix).build()
        sub_builders.append((pname, nb))
        combined.absorb(nb)
        node_to_transition_global.update(nb.node_to_transition)

    # --- avvio unico: un solo posto sorgente marcato, che alimenta lo
    #     start event di OGNI pool nello stesso istante logico ---
    start_place = combined.new_place("Avvio collaborazione")
    start_trans = combined.new_transition("Avvia collaborazione")
    combined.arcs.append((start_place, start_trans))
    for pname, nb in sub_builders:
        combined.arcs.append((start_trans, nb.initial_place))
    combined.initial_place = start_place

    # --- fine unica: un solo posto pozzo, raggiunto solo quando OGNI pool
    #     ha raggiunto il proprio end event (AND-join) ---
    end_trans = combined.new_transition("Fine collaborazione")
    for pname, nb in sub_builders:
        for fp in nb.final_places:
            combined.arcs.append((fp, end_trans))
    end_place = combined.new_place("Fine collaborazione")
    combined.arcs.append((end_trans, end_place))
    combined.final_places.append(end_place)

    # --- un posto per ogni DESTINATARIO di messageFlow (non uno per ogni
    #     messageFlow!): se piu' messageFlow diversi convergono sullo stesso
    #     evento di catch (caso tipico: un catch "generico" che puo' ricevere
    #     uno fra piu' tipi di messaggio alternativi, es. "Ricevi risposta
    #     disponibilita'" che puo' arrivare da 3 send diversi a seconda di
    #     come l'altra parte ha risposto), devono condividere UN SOLO posto
    #     "e' arrivato un messaggio", alimentato da piu' mittenti alternativi.
    #     Se si creasse un posto per ogni singolo messageFlow e li si desse
    #     TUTTI in ingresso alla stessa transizione di catch, la si
    #     costringerebbe ad aspettare TUTTI i messaggi insieme invece che
    #     uno qualsiasi: esattamente lo stesso tipo di errore (AND al posto
    #     di scelta libera) diagnosticato nel vecchio PetriNet/collaboration.pnml
    #     per i gateway XOR. ---
    missing = []
    by_target = {}
    for mf_id, src, tgt in message_flows:
        by_target.setdefault(tgt, []).append((mf_id, src))

    for tgt, senders in by_target.items():
        tgt_t = node_to_transition_global.get(tgt)
        if tgt_t is None:
            for mf_id, src in senders:
                missing.append((mf_id, src, tgt))
            continue
        tgt_label = combined.transitions.get(tgt_t, tgt)
        src_transitions = []
        for mf_id, src in senders:
            src_t = node_to_transition_global.get(src)
            if src_t is None:
                missing.append((mf_id, src, tgt))
                continue
            if src_t not in src_transitions:
                src_transitions.append(src_t)
        if not src_transitions:
            continue
        src_labels = " / ".join(dict.fromkeys(combined.transitions.get(t, t) for t in src_transitions))
        label = "Msg a '" + tgt_label + "' da: " + src_labels
        mp = combined.new_place(label)
        for src_t in src_transitions:
            combined.arcs.append((src_t, mp))
        combined.arcs.append((mp, tgt_t))

    if missing:
        for mf_id, src, tgt in missing:
            print("ATTENZIONE: messageFlow " + mf_id + " (" + str(src) + " -> " + str(tgt)
                  + ") non collegato: sorgente o destinazione non e' un nodo 'hard' tradotto.")

    _fix_ricevi_risposta_disponibilita_correlation(combined, node_to_transition_global)

    write_pnml(combined, out_path, net_id, net_name)
    return combined


def _fix_ricevi_risposta_disponibilita_correlation(combined, node_to_transition_global):
    """Correzione manuale mirata (richiede conoscenza di dominio non
    deducibile dal solo diagramma di controllo BPMN).

    L'evento di catch 'Ricevi risposta disponibilitá' (Event_0l4idya, pool
    Alex) riceve 3 messageFlow alternativi da Bob (Proponi data e tipologia
    attrezzatura / Delega scelta / Comunica indisponibilitá), tutti verso lo
    stesso catch event. Il gateway XOR subito dopo (Gateway_0537l1d
    "Appuntamento annullato?" + Gateway_1r5qul7 "Tipo di risposta?", fusi
    dalla traduzione standard in un unico posto condiviso con 3 transizioni
    alternative) NELLA REALTA' del processo instrada in modo deterministico
    in base a quale messaggio e' arrivato:
        Comunica indisponibilitá  -> "Annullato? = Si" (fine)
        Proponi data ...          -> "Proposta ricevuta"
        Delega scelta             -> "Scelta delegata" (Invia nuova proposta)
    Una rete P/T "piatta" (senza colori/dati) non puo' rappresentare questa
    correlazione se i 3 messaggi confluiscono in un unico posto "arrivato un
    messaggio": il gateway a valle diventa una scelta libera che ammette
    ANCHE le combinazioni sbagliate (es. indisponibilitá trattata come
    proposta ricevuta), generando esecuzioni spurie che il processo reale
    non permette mai. Questo e' esattamente cio' che l'analisi di soundness
    segnala come 3 posti non limitati.

    Fix: si "sdoppia" (unfolding) il segmento catch+gateway in 3 varianti
    dedicate, una per mittente, ciascuna con IN il proprio posto-messaggio
    dedicato e OUT forzato verso il SOLO ramo corretto per quel mittente. Il
    vecchio catch condiviso e il vecchio posto-gateway condiviso vengono
    rimossi, cosi' la combinazione sbagliata non e' piu' rappresentabile.

    Se in futuro il BPMN dovesse cambiare struttura, questa funzione fallisce
    silenziosamente (con un avviso) invece di applicare una correzione
    sbagliata: chi la mantiene deve aggiornarla a mano insieme al diagramma.
    """
    CATCH_ID = "Event_0l4idya"
    SENDER_INDISPONIBILE = "Activity_1u6e7jx"    # Comunica indisponibilitá
    SENDER_PROPOSTA = "Activity_0kkgc07"          # Proponi data e tipologia attrezzatura
    SENDER_DELEGA = "Activity_0jpe9cp"            # Delega scelta
    SCELTA_DELEGATA_TARGET = "Activity_1hac249"   # Invia nuova proposta

    old_catch_t = node_to_transition_global.get(CATCH_ID)
    sender_t = {
        "indisponibile": node_to_transition_global.get(SENDER_INDISPONIBILE),
        "proposta": node_to_transition_global.get(SENDER_PROPOSTA),
        "delega": node_to_transition_global.get(SENDER_DELEGA),
    }
    delega_target_t = node_to_transition_global.get(SCELTA_DELEGATA_TARGET)

    if old_catch_t is None or delega_target_t is None or any(v is None for v in sender_t.values()):
        print("ATTENZIONE: _fix_ricevi_risposta_disponibilita_correlation: id BPMN attesi "
              "non trovati, correzione NON applicata (il diagramma potrebbe essere cambiato).")
        return

    old_ins = [s for s, t in combined.arcs if t == old_catch_t]
    old_outs = [t for s, t in combined.arcs if s == old_catch_t]
    if len(old_outs) != 1 or len(old_ins) != 2:
        print("ATTENZIONE: struttura del catch diversa da quella attesa, correzione NON applicata.")
        return
    shared_gateway_place = old_outs[0]

    expected_senders = set(sender_t.values())
    old_msg_place = None
    control_place = None
    for p in old_ins:
        producers = {s for s, t in combined.arcs if t == p}
        if producers == expected_senders:
            old_msg_place = p
        else:
            control_place = p
    if old_msg_place is None or control_place is None:
        print("ATTENZIONE: posti in ingresso al catch diversi da quelli attesi, correzione NON applicata.")
        return

    downstream = [t for s, t in combined.arcs if s == shared_gateway_place]

    def branch_by_label(substr):
        for t in downstream:
            if substr in combined.transitions.get(t, ""):
                return t
        return None

    si_branch_t = branch_by_label("annullato? (Si)")
    proposta_branch_t = branch_by_label("Proposta ricevuta")
    if si_branch_t is None or proposta_branch_t is None or delega_target_t not in downstream:
        print("ATTENZIONE: rami del gateway diversi da quelli attesi, correzione NON applicata.")
        return

    si_out = [t for s, t in combined.arcs if s == si_branch_t]
    proposta_out = [t for s, t in combined.arcs if s == proposta_branch_t]

    # --- rimuovi il vecchio catch condiviso, il vecchio posto-gateway
    #     condiviso, i vecchi rami "Annullato (Si)"/"Proposta ricevuta" (ormai
    #     sostituiti dalle 3 varianti dedicate) e TUTTI i loro archi (sia in
    #     ingresso sia in uscita: una transizione lasciata senza posti in
    #     ingresso scatterebbe all'infinito senza precondizioni, generando
    #     token dal nulla) ---
    obsolete_trans = {si_branch_t, proposta_branch_t} - {delega_target_t}
    combined.arcs = [
        (s, t) for s, t in combined.arcs
        if s != old_catch_t and t != old_catch_t
        and s != shared_gateway_place
        and s != old_msg_place and t != old_msg_place
        and s not in obsolete_trans and t not in obsolete_trans
    ]
    del combined.transitions[old_catch_t]
    del combined.places[old_msg_place]
    del combined.places[shared_gateway_place]
    for t in obsolete_trans:
        del combined.transitions[t]

    # --- crea le 3 varianti dedicate, ciascuna forzata sul ramo corretto ---
    for tag, out_places in (("indisponibile", si_out), ("proposta", proposta_out)):
        st = sender_t[tag]
        mp = combined.new_place("Msg (" + tag + "): Ricevi risposta disponibilitá")
        combined.arcs.append((st, mp))
        vt = combined.new_transition("Ricevi risposta disponibilitá (" + tag + ")")
        combined.arcs.append((control_place, vt))
        combined.arcs.append((mp, vt))
        for op in out_places:
            combined.arcs.append((vt, op))

    st = sender_t["delega"]
    mp = combined.new_place("Msg (delega): Ricevi risposta disponibilitá")
    combined.arcs.append((st, mp))
    vt = combined.new_transition("Ricevi risposta disponibilitá (delega)")
    combined.arcs.append((control_place, vt))
    combined.arcs.append((mp, vt))
    dp = combined.new_place("Scelta delegata confermata")
    combined.arcs.append((vt, dp))
    combined.arcs.append((dp, delega_target_t))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: bpmn_to_pnml.py input.bpmn output.pnml [nome_rete] [prefix]")
        sys.exit(1)
    bpmn_path = sys.argv[1]
    out_path = sys.argv[2]
    net_name = sys.argv[3] if len(sys.argv) > 3 else "Rete generata"
    prefix = sys.argv[4] if len(sys.argv) > 4 else ""

    _tree = ET.parse(bpmn_path)
    _n_proc = len(_tree.getroot().findall("bpmn:process", NS))
    if _n_proc >= 2:
        nb = convert_collaboration(bpmn_path, out_path, net_name)
    else:
        nb = convert(bpmn_path, out_path, net_name, prefix=prefix)
    print("Posti: " + str(len(nb.places)) + "  Transizioni: " + str(len(nb.transitions)) + "  Archi: " + str(len(nb.arcs)))
