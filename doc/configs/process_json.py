import json
import pathlib

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

# TODO: expand to all configs
algorithm = "article-rank"
conf_filename = pathlib.Path("json") / f"{algorithm}.json"
adoc_root = pathlib.Path("..") / "modules" / "ROOT" / "partials"

with open(conf_filename) as conf_file:
    conf_json = json.load(conf_file)
    adoc_filename = adoc_root / conf_json["page_path"] / "specific-configuration.adoc"

    with open(adoc_filename, "w") as adoc_file:
      adoc_file.write("// DO NOT EDIT: File generated automatically\n")
        
      for conf in conf_json["config"]:
          name, type_, default, optional, description = conf["name"], conf["type"], conf["default"], conf["optional"], conf["description"]
          if name in LINKS:
              name = BASE_LINK + LINKS[name] + f"[{name}]"
          type_ = " or ".join(type_) if isinstance(type_, list) else type_
          optional = "yes" if optional else "no"
          
          line = f"| {name} | {type_} | {default} | {optional} | {description}"
          adoc_file.write(line + "\n")
