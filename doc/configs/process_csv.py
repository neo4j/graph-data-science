import csv
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

conf_filename = pathlib.Path("csv") / f"{algorithm}.csv"
adoc_filename = pathlib.Path("..") / "modules" / "ROOT" / "partials" / "algorithms" / algorithm / "specific-configuration.adoc"

with open(conf_filename) as conf, open(adoc_filename, "w") as adoc:
    csvreader = csv.DictReader(conf)
    adoc.write("// DO NOT EDIT: File generated automatically\n")
      
    for row in csvreader:
        name, type_, default, optional, description = row['name'], row['type'], row['default'], row['optional'], row['description']
        if name in LINKS:
            name = BASE_LINK + LINKS[name] + f"[{name}]"
        
        line = f"| {name} | {type_} | {default} | {optional} | {description}"
        adoc.write(line + "\n")
