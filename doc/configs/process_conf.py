import json
from pathlib import Path

BASE_LINK = "xref:common-usage/running-algos.adoc#"
LINKS = {
    "concurrency": "common-configuration-concurrency",
    "nodeLabels": "common-configuration-node-labels",
    "relationshipTypes": "common-configuration-relationship-types",
    "nodeWeightProperty": "common-configuration-node-weight-property",
    "relationshipWeightProperty": "common-configuration-relationship-weight-property",
    "maxIterations": "common-configuration-max-iterations",
    "tolerance": "common-configuration-tolerance",
    "seedProperty": "common-configuration-seed-property",
    "writeProperty": "common-configuration-write-property",
    "writeConcurrency": "common-configuration-write-concurrency",
    "jobId": "common-configuration-jobid",
    "logProgress": "common-configuration-logProgress",
}

INCLUDED_ALGORITHMS = {
    "Article Rank",
    "Betweenness Centrality",
    # "CELF",
    # "Closeness Centrality",
    "Degree Centrality",
    "Eigenvector Centrality",
    "PageRank",
    # "Harmonic Centrality",
    # "HITS",
    "Conductance metric",
    # "K-Core Decomposition",
    # "K-1 Coloring",
    # "K-Means Clustering",
    # "Label Propagation",
    # "Leiden",
    "Local Clustering Coefficient",
    # "Louvain",
    "Modularity metric",
    # "Modularity Optimization",
    # "Strongly Connected Components",
    "Triangle Count",
    # "Weakly Connected Components",
    # "Approximate Maximum k-cut",
    # "Speaker-Listener Label Propagation",
    "Node Similarity",
    # "Filtered Node Similarity",
    # "K-Nearest Neighbors",
    # "Filtered K-Nearest Neighbors",
    # "Delta-Stepping Single-Source Shortest Path",
    # "Dijkstra Source-Target Shortest Path",
    # "Dijkstra Single-Source Shortest Path",
    # "A* Shortest Path",
    # "Yen's Shortest Path algorithm",
    # "Minimum Weight Spanning Tree",
    # "Minimum Directed Steiner Tree",
    # "Random Walk",
    "Breadth First Search",
    "Depth First Search",
    # "Bellman-Ford Single-Source Shortest Path",
    # "Longest Path for DAG",
    # "All Pairs Shortest Path",
    # "Topological Sort",
    # "Longest Path for DAG",
    "Fast Random Projection",
    "GraphSAGE",
    "Node2Vec",
    "HashGNN",
}

conf_filename = "algorithms-conf.json"
adoc_root = Path("..") / "modules" / "ROOT" / "partials"

with open(conf_filename) as conf_file:
    conf_json = json.load(conf_file)

    for algo in conf_json["algorithms"]:
        # TODO: remove when all algorithms are included
        if algo["name"] not in INCLUDED_ALGORITHMS:
            continue

        adoc_filename = adoc_root / algo["page_path"] / "specific-configuration.adoc"

        with open(adoc_filename, "w") as adoc_file:
            adoc_file.write(
                f"// DO NOT EDIT: File generated automatically by the {Path(__file__).name} script\n"
            )

            for conf in algo["config"]:
                name, type_, default, optional, description = (
                    conf["name"],
                    conf["type"],
                    conf["default"],
                    conf["optional"],
                    conf["description"],
                )
                if name in LINKS:
                    name = BASE_LINK + LINKS[name] + f"[{name}]"
                type_ = " or ".join(type_) if isinstance(type_, list) else type_
                optional = "yes" if optional else "no"
                default = "null" if default is None else default

                line = f"| {name} | {type_} | {default} | {optional} | {description}"
                adoc_file.write(line + "\n")

            for note in algo.get("config_notes", []):
                adoc_file.write(f"5+| {note}\n")
