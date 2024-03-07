# Procedures

This directory hosts the procedures (UI) layer in the standard stack [citation needed]:

1) User interface
2) Applications
3) Domain
4) Integration

In this context, we have [the extension (the mechanisms for integration with Neo4j)](extension/README.md) on top, which is super thin: just edition-specific choices and delegation to some reusable code.

The reusable code lives in [a module named integration](integration/README.md), and so is usable if we build another extension.

The integration module in turn uses the [top level procedure facade](facade/README.md), and eventually the functionality in [the GDS application layer](../applications/README.md).

One squiggle to note is pipelines. They are a first class thing hanging off of the top level procedure facade. But they _use_ [algorithm procedures](algorithms-facade/README.md), hence this split.
